import os
import sys
from functools import wraps
from datetime import datetime, timezone, timedelta
from flask import Flask, request, abort, render_template, redirect, url_for, Response, jsonify

from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.exceptions import (
    InvalidSignatureError, LineBotApiError
)
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage, FollowEvent,
    QuickReply, QuickReplyButton, MessageAction
)

from sqlalchemy import create_engine, Column, String, DateTime, func, Integer, Text, or_
from sqlalchemy.orm import sessionmaker, declarative_base

app = Flask(__name__)

# --- 環境変数から設定を取得 ---
db_url = os.environ.get('DATABASE_URL')
admin_username = os.environ.get('ADMIN_USERNAME')
admin_password = os.environ.get('ADMIN_PASSWORD')

if db_url and db_url.startswith("postgres://"):
    database_url = db_url.replace("postgres://", "postgresql+psycopg://", 1)
else:
    database_url = db_url

if not all([database_url, admin_username, admin_password]):
    print("!!! エラー: 必要な環境変数が設定されていません。")
    sys.exit(1)

# --- データベースの設定 ---
Base = declarative_base()
class User(Base):
    __tablename__ = 'users'
    id = Column(String, primary_key=True)
    display_name = Column(String)
    nickname = Column(String)
    tags = Column(String, default="")
    status = Column(String, default="未対応")
    sent_steps = Column(String, default="")
    created_at = Column(DateTime, server_default=func.now())

class StepMessage(Base):
    __tablename__ = 'step_messages'
    id = Column(Integer, primary_key=True, autoincrement=True)
    days_after = Column(Integer, nullable=False)
    message_text = Column(Text, nullable=False)

class Setting(Base):
    __tablename__ = 'settings'
    key = Column(String, primary_key=True)
    value = Column(Text)

class Tag(Base):
    __tablename__ = 'tags'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False, unique=True)

class Message(Base):
    __tablename__ = 'messages'
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String, nullable=False)
    sender_type = Column(String, nullable=False)
    content = Column(Text, nullable=False)
    created_at = Column(DateTime, server_default=func.now())

class ScheduledMessage(Base):
    __tablename__ = 'scheduled_messages'
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String, nullable=False)
    message_text = Column(Text, nullable=False)
    send_at = Column(DateTime, nullable=False)
    status = Column(String, default='pending')

class BatchRunLog(Base):
    __tablename__ = 'batch_run_log'
    id = Column(Integer, primary_key=True)
    last_step_check_date = Column(DateTime, nullable=False)


try:
    engine = create_engine(database_url, pool_pre_ping=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
except Exception as e:
    print(f"!!! データベース接続エラー: {e}")
    sys.exit(1)

# --- LINE APIのクライアントを動的に生成する関数 ---
def get_line_bot_api():
    session = Session()
    access_token_setting = session.query(Setting).filter_by(key='line_channel_access_token').first()
    session.close()
    if access_token_setting and access_token_setting.value:
        return LineBotApi(access_token_setting.value)
    return None

# --- ベーシック認証用のコード ---
def check_auth(username, password):
    return username == admin_username and password == admin_password

def authenticate():
    return Response(
    'Could not verify your access level for that URL.\n'
    'You have to login with proper credentials', 401,
    {'WWW-Authenticate': 'Basic realm="Login Required"'})

def auth_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated

# --- ▼▼▼ 執事（バッチ処理）のコードをここに統合 ▼▼▼ ---
def process_step_messages(session, line_bot_api):
    print("--- ステップ配信のチェック開始 ---")
    today = datetime.utcnow().date()
    log = session.query(BatchRunLog).first()
    if log and log.last_step_check_date.date() == today:
        print("本日のステップ配信は既にチェック済みです。")
        return
    scenarios = session.query(StepMessage).all()
    if not scenarios:
        print("処理すべきステップ配信シナリオはありません。")
        return
    for scenario in scenarios:
        target_date = today - timedelta(days=scenario.days_after)
        target_users = session.query(User).filter(func.date(User.created_at) == target_date).all()
        if not target_users:
            continue
        users_to_send = []
        for user in target_users:
            sent_steps_list = user.sent_steps.split(',')
            if str(scenario.days_after) not in sent_steps_list:
                users_to_send.append(user)
        if users_to_send:
            user_ids_to_send = [user.id for user in users_to_send]
            try:
                print(f"登録{scenario.days_after}日後の{len(user_ids_to_send)}人にメッセージを送信します...")
                line_bot_api.multicast(user_ids_to_send, TextSendMessage(text=scenario.message_text))
                for user in users_to_send:
                    user.sent_steps += f"{scenario.days_after},"
                session.commit()
                print("送信記録をデータベースに保存しました。")
            except LineBotApiError as e:
                print(f"!!! ステップ配信(ID: {scenario.id})の送信でエラー: {e}")
                session.rollback()
    if log:
        log.last_step_check_date = datetime.utcnow()
    else:
        new_log = BatchRunLog(last_step_check_date=datetime.utcnow())
        session.add(new_log)
    session.commit()

def process_scheduled_messages(session, line_bot_api):
    print("--- 予約投稿のチェック開始 ---")
    now = datetime.utcnow()
    messages_to_send = session.query(ScheduledMessage).filter(
        ScheduledMessage.status == 'pending',
        ScheduledMessage.send_at <= now
    ).all()
    if not messages_to_send:
        print("送信すべき予約投稿はありません。")
        return
    for msg in messages_to_send:
        try:
            print(f"予約投稿を送信します (To: {msg.user_id})")
            line_bot_api.push_message(msg.user_id, TextSendMessage(text=msg.message_text))
            msg.status = 'sent'
        except LineBotApiError as e:
            print(f"!!! 予約投稿(ID: {msg.id})の送信でエラー: {e}")
            msg.status = 'error'
    session.commit()
    print(f"{len(messages_to_send)}件の予約投稿を処理しました。")

@app.route("/trigger-batch")
def trigger_batch_route():
    line_bot_api = get_line_bot_api()
    if not line_bot_api:
        return "LINE API not configured.", 500
    session = Session()
    try:
        process_step_messages(session, line_bot_api)
        process_scheduled_messages(session, line_bot_api)
    finally:
        session.close()
    return "Batch process triggered successfully.", 200
# --- ▲▲▲ 執事（バッチ処理）のコード ▲▲▲ ---


# --- 管理画面用のコード ---
@app.route("/admin/")
@auth_required
def admin_dashboard():
    return redirect(url_for('admin_friends_page'))

@app.route("/admin/friends")
@auth_required
def admin_friends_page():
    session = Session()
    search_query = request.args.get('q', '')
    query = session.query(User)
    if search_query:
        query = query.filter(
            or_(
                User.display_name.like(f'%{search_query}%'),
                User.nickname.like(f'%{search_query}%')
            )
        )
    all_users = query.order_by(User.created_at.desc()).all()
    session.close()
    return render_template('friends.html', users=all_users, search_query=search_query)

@app.route("/admin/steps")
@auth_required
def admin_steps_page():
    session = Session()
    step_messages = session.query(StepMessage).order_by(StepMessage.days_after).all()
    session.close()
    return render_template('steps.html', step_messages=step_messages)

@app.route("/admin/messaging")
@auth_required
def admin_messaging_page():
    return render_template('messaging.html')
    
@app.route("/admin/tags", methods=['GET', 'POST'])
@auth_required
def admin_tags_page():
    session = Session()
    if request.method == 'POST':
        tag_name = request.form.get('tag_name')
        if tag_name:
            existing_tag = session.query(Tag).filter_by(name=tag_name).first()
            if not existing_tag:
                new_tag = Tag(name=tag_name)
                session.add(new_tag)
                session.commit()
        return redirect(url_for('admin_tags_page'))
    all_tags = session.query(Tag).order_by(Tag.name).all()
    session.close()
    return render_template('tags.html', tags=all_tags)

@app.route("/delete-tag/<int:tag_id>", methods=['POST'])
@auth_required
def delete_tag(tag_id):
    session = Session()
    tag_to_delete = session.query(Tag).filter_by(id=tag_id).first()
    if tag_to_delete:
        session.delete(tag_to_delete)
        session.commit()
    session.close()
    return redirect(url_for('admin_tags_page'))

@app.route("/admin/settings", methods=['GET', 'POST'])
@auth_required
def admin_settings_page():
    session = Session()
    if request.method == 'POST':
        settings_keys = ['line_channel_access_token', 'line_channel_secret']
        for key in settings_keys:
            setting = session.query(Setting).filter_by(key=key).first()
            if not setting:
                setting = Setting(key=key)
                session.add(setting)
            setting.value = request.form.get(key)
        session.commit()
        return redirect(url_for('admin_settings_page'))
    token_setting = session.query(Setting).filter_by(key='line_channel_access_token').first()
    secret_setting = session.query(Setting).filter_by(key='line_channel_secret').first()
    session.close()
    return render_template('settings.html', token=token_setting, secret=secret_setting)

@app.route("/admin/chat")
@auth_required
def admin_chat_page():
    session = Session()
    status_filter = request.args.get('status')
    search_query = request.args.get('q', '')
    query = session.query(User)
    if status_filter:
        query = query.filter(User.status == status_filter)
    if search_query:
        query = query.join(Message, User.id == Message.user_id).filter(
            or_(
                User.display_name.like(f'%{search_query}%'),
                User.nickname.like(f'%{search_query}%'),
                Message.content.like(f'%{search_query}%')
            )
        ).distinct()
    all_users = query.order_by(User.created_at.desc()).all()
    latest_message_subq = session.query(
        Message.user_id,
        func.max(Message.created_at).label('max_created_at')
    ).group_by(Message.user_id).subquery()
    latest_messages_q = session.query(Message).join(
        latest_message_subq,
        (Message.user_id == latest_message_subq.c.user_id) &
        (Message.created_at == latest_message_subq.c.max_created_at)
    )
    latest_messages = {msg.user_id: msg for msg in latest_messages_q}
    session.close()
    return render_template('chat.html', users=all_users, current_filter=status_filter, search_query=search_query, latest_messages=latest_messages)

@app.route("/admin/chat/<user_id>")
@auth_required
def admin_chat_detail_page(user_id):
    session = Session()
    user = session.query(User).filter_by(id=user_id).first()
    messages = session.query(Message).filter_by(user_id=user_id).order_by(Message.created_at).all()
    session.close()
    if not user:
        return "ユーザーが見つかりません。", 404
    return render_template('chat_detail.html', user=user, messages=messages)

@app.route("/admin/chat/<user_id>/send", methods=['POST'])
@auth_required
def send_reply(user_id):
    line_bot_api = get_line_bot_api()
    if not line_bot_api: return "アクセストークンが設定されていません。", 500
    reply_text = request.form.get('message_text')
    if not reply_text:
        return redirect(url_for('admin_chat_detail_page', user_id=user_id))
    try:
        line_bot_api.push_message(user_id, TextSendMessage(text=reply_text))
    except LineBotApiError as e:
        print(f"!!! 個別返信の送信でエラー: {e}")
        return "LINEへのメッセージ送信に失敗しました。", 500
    session = Session()
    new_message = Message(user_id=user_id, sender_type='admin', content=reply_text)
    session.add(new_message)
    session.commit()
    session.close()
    return redirect(url_for('admin_chat_detail_page', user_id=user_id))
    
@app.route("/admin/chat/<user_id>/schedule", methods=['POST'])
@auth_required
def schedule_reply(user_id):
    message_text = request.form.get('message_text')
    send_at_str = request.form.get('send_at')
    if not message_text or not send_at_str:
        return redirect(url_for('admin_chat_detail_page', user_id=user_id))
    
    naive_dt = datetime.fromisoformat(send_at_str)
    jst = timezone(timedelta(hours=9))
    jst_dt = naive_dt.astimezone(jst)
    utc_dt = jst_dt.astimezone(timezone.utc)

    session = Session()
    new_scheduled_message = ScheduledMessage(
        user_id=user_id,
        message_text=message_text,
        send_at=utc_dt,
        status='pending'
    )
    session.add(new_scheduled_message)
    session.commit()
    session.close()
    return redirect(url_for('admin_chat_detail_page', user_id=user_id))

@app.route("/admin/scheduled")
@auth_required
def admin_scheduled_page():
    session = Session()
    scheduled_messages = session.query(ScheduledMessage, User.display_name).join(
        User, ScheduledMessage.user_id == User.id
    ).filter(ScheduledMessage.status == 'pending').order_by(ScheduledMessage.send_at).all()
    session.close()
    return render_template('scheduled.html', messages=scheduled_messages)

@app.route("/edit-scheduled/<int:msg_id>", methods=['GET', 'POST'])
@auth_required
def edit_scheduled_page(msg_id):
    session = Session()
    message_to_edit = session.query(ScheduledMessage).filter_by(id=msg_id).first()
    if not message_to_edit:
        return "メッセージが見つかりません。", 404
    if request.method == 'POST':
        message_to_edit.message_text = request.form.get('message_text')
        send_at_str = request.form.get('send_at')
        if send_at_str:
            naive_dt = datetime.fromisoformat(send_at_str)
            jst = timezone(timedelta(hours=9))
            jst_dt = naive_dt.astimezone(jst)
            utc_dt = jst_dt.astimezone(timezone.utc)
            message_to_edit.send_at = utc_dt
        session.commit()
        session.close()
        return redirect(url_for('admin_scheduled_page'))
    
    jst = timezone(timedelta(hours=9))
    message_to_edit.send_at_jst = message_to_edit.send_at.astimezone(jst)
    session.close()
    return render_template('edit_scheduled.html', message=message_to_edit)

@app.route("/delete-scheduled/<int:msg_id>", methods=['POST'])
@auth_required
def delete_scheduled(msg_id):
    session = Session()
    message_to_delete = session.query(ScheduledMessage).filter_by(id=msg_id).first()
    if message_to_delete:
        session.delete(message_to_delete)
        session.commit()
    session.close()
    return redirect(url_for('admin_scheduled_page'))

@app.route("/update-status/<user_id>", methods=['POST'])
@auth_required
def update_status(user_id):
    new_status = request.form.get('status')
    session = Session()
    user = session.query(User).filter_by(id=user_id).first()
    if user and new_status:
        user.status = new_status
        session.commit()
    session.close()
    return redirect(url_for('admin_chat_detail_page', user_id=user_id))

@app.route("/edit-user/<user_id>")
@auth_required
def edit_user_page(user_id):
    session = Session()
    user = session.query(User).filter_by(id=user_id).first()
    all_tags = session.query(Tag).all()
    session.close()
    if not user:
        return "ユーザーが見つかりません。", 404
    return render_template('edit_user.html', user=user, all_tags=all_tags)

@app.route("/update-user/<user_id>", methods=['POST'])
@auth_required
def update_user(user_id):
    session = Session()
    user = session.query(User).filter_by(id=user_id).first()
    if user:
        user.nickname = request.form.get('nickname')
        selected_tags = request.form.getlist('tags')
        user.tags = ",".join(selected_tags) + ("," if selected_tags else "")
        session.commit()
    session.close()
    return jsonify({'status': 'success'})

@app.route("/add-step", methods=['POST'])
@auth_required
def add_step():
    days_after = request.form.get('days_after', type=int)
    message_text = request.form.get('message_text')
    if days_after is not None and message_text:
        session = Session()
        new_step = StepMessage(days_after=days_after, message_text=message_text)
        session.add(new_step)
        session.commit()
        session.close()
    return redirect(url_for('admin_steps_page'))

@app.route("/delete-step/<int:step_id>", methods=['POST'])
@auth_required
def delete_step(step_id):
    session = Session()
    step_to_delete = session.query(StepMessage).filter_by(id=step_id).first()
    if step_to_delete:
        session.delete(step_to_delete)
        session.commit()
    session.close()
    return redirect(url_for('admin_steps_page'))

# --- LINE Bot本体の機能 ---
@app.route("/callback", methods=['POST'])
def callback():
    session = Session()
    channel_secret_setting = session.query(Setting).filter_by(key='line_channel_secret').first()
    session.close()
    if not channel_secret_setting or not channel_secret_setting.value:
        print("チャネルシークレットがDBに設定されていないため、リクエストを無視します。")
        return "OK"
    handler = WebhookHandler(channel_secret_setting.value)

    @handler.add(FollowEvent)
    def handle_follow(event):
        line_bot_api = get_line_bot_api()
        if not line_bot_api: return
        user_id = event.source.user_id
        session = Session()
        try:
            profile = line_bot_api.get_profile(user_id)
            display_name = profile.display_name
        except LineBotApiError as e:
            print(f"!!! プロフィール取得でエラー: {e}")
            display_name = "取得失敗"
        existing_user = session.query(User).filter_by(id=user_id).first()
        if not existing_user:
            new_user = User(id=user_id, display_name=display_name)
            session.add(new_user)
            session.commit()
            print(f"新しいユーザーが追加されました: {user_id} ({display_name})")
        session.close()

    @handler.add(MessageEvent, message=TextMessage)
    def handle_message(event):
        line_bot_api = get_line_bot_api()
        if not line_bot_api: return
        user_id = event.source.user_id
        user_message = event.message.text
        session = Session()
        new_message = Message(user_id=user_id, sender_type='user', content=user_message)
        session.add(new_message)
        session.commit()
        user = session.query(User).filter_by(id=user_id).first()
        if user_message == "アンケート":
            quick_reply_buttons = QuickReply(items=[QuickReplyButton(action=MessageAction(label="はい", text="はい")), QuickReplyButton(action=MessageAction(label="いいえ", text="いいえ"))])
            reply_message = TextSendMessage(text="サービスに満足していますか？", quick_reply=quick_reply_buttons)
            line_bot_api.reply_message(event.reply_token, reply_message)
        elif user_message == "はい":
            if user and "satisfied" not in user.tags:
                user.tags += "satisfied,"
                session.commit()
                reply_text = "ありがとうございます！ご回答を記録しました。"
            else:
                reply_text = "ご回答ありがとうございます！"
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
        elif user_message == "いいえ":
            if user and "unsatisfied" not in user.tags:
                user.tags += "unsatisfied,"
                session.commit()
                reply_text = "ご意見ありがとうございます。今後の参考にさせていただきます。"
            else:
                reply_text = "ご意見ありがとうございます。"
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
        elif user_message == "クーポン":
            if user and "coupon" not in user.tags:
                user.tags += "coupon,"
                session.commit()
                reply_text = "クーポン希望者として登録しました！"
            else:
                reply_text = "すでに登録済みです。"
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
        session.close()

    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'

@app.route("/", methods=['GET'])
def health_check():
    return 'OK'

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)