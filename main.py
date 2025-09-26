import os
import sys
from functools import wraps
from datetime import datetime, timezone, timedelta
import json
from flask import Flask, request, abort, render_template, redirect, url_for, Response, jsonify, send_from_directory
from werkzeug.utils import secure_filename
from uuid import uuid4
from dotenv import load_dotenv

from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.exceptions import (
    InvalidSignatureError, LineBotApiError
)
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage, FollowEvent,
    QuickReply, QuickReplyButton, MessageAction, ImageSendMessage,
    TemplateSendMessage, ButtonsTemplate, CarouselTemplate, CarouselColumn, URIAction,
    ImagemapSendMessage, BaseSize, ImagemapArea, URIImagemapAction, MessageImagemapAction
)

from sqlalchemy import create_engine, Column, String, DateTime, func, Integer, Text, or_, and_
from sqlalchemy.orm import sessionmaker, declarative_base

# .envファイルをロード
load_dotenv()
app = Flask(__name__)

# --- ファイルアップロードの設定 ---
UPLOAD_FOLDER = 'static/uploads'
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# --- 環境変数から設定を取得 ---
db_url_from_env = os.environ.get('DATABASE_URL')
admin_username = os.environ.get('ADMIN_USERNAME')
admin_password = os.environ.get('ADMIN_PASSWORD')

if db_url_from_env and db_url_from_env.startswith("postgres://"):
    database_url = db_url_from_env.replace("postgres://", "postgresql+psycopg://", 1)
else:
    database_url = db_url_from_env

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
    send_at = Column(DateTime(timezone=True), nullable=False)
    status = Column(String, default='pending')

class ScheduledBroadcast(Base):
    __tablename__ = 'scheduled_broadcasts'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, default="無題の配信")
    targeting_info = Column(Text, nullable=False)
    messages_info = Column(Text, nullable=False)
    send_at = Column(DateTime(timezone=True), nullable=False)
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
    
# --- アップロードされたファイルを配信するための関数 ---
@app.route('/uploads/<filename>')
def uploaded_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)

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
    session = Session()
    all_tags = session.query(Tag).order_by(Tag.name).all()
    scheduled_broadcasts = session.query(ScheduledBroadcast).filter(
        ScheduledBroadcast.status == 'pending'
    ).order_by(ScheduledBroadcast.send_at).all()
    
    jst = timezone(timedelta(hours=9))
    for broadcast in scheduled_broadcasts:
        # DBから取得したUTC時刻を、正しくJSTに変換
        broadcast.send_at_jst = broadcast.send_at.astimezone(jst)

    session.close()
    return render_template('messaging.html', tags=all_tags, broadcasts=scheduled_broadcasts)
    
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
    scheduled_messages = session.query(ScheduledMessage).filter_by(
        user_id=user_id, status='pending'
    ).order_by(ScheduledMessage.send_at).all()
    session.close()
    if not user:
        return "ユーザーが見つかりません。", 404
    return render_template('chat_detail.html', user=user, messages=messages, scheduled_messages=scheduled_messages)

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
    
    try:
        naive_dt = datetime.strptime(send_at_str, '%Y-%m-%d %H:%M')
    except ValueError:
        return "日時の形式が正しくありません。", 400

    jst = timezone(timedelta(hours=9))
    jst_dt = naive_dt.replace(tzinfo=jst)
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

@app.route("/edit-scheduled/<int:msg_id>", methods=['GET', 'POST'])
@auth_required
def edit_scheduled_page(msg_id):
    session = Session()
    message_to_edit = session.query(ScheduledMessage).filter_by(id=msg_id).first()
    if not message_to_edit:
        session.close()
        return "メッセージが見つかりません。", 404

    user_id_for_redirect = message_to_edit.user_id
    jst = timezone(timedelta(hours=9))
    
    if request.method == 'POST':
        message_to_edit.message_text = request.form.get('message_text')
        send_at_str = request.form.get('send_at')
        if send_at_str:
            try:
                naive_dt = datetime.strptime(send_at_str, '%Y-%m-%d %H:%M')
                jst_dt = naive_dt.replace(tzinfo=jst)
                utc_dt = jst_dt.astimezone(timezone.utc)
                message_to_edit.send_at = utc_dt
            except ValueError:
                return "日時の形式が正しくありません。", 400
        session.commit()
        session.close()
        return jsonify({'status': 'success'})

    utc_dt = message_to_edit.send_at.replace(tzinfo=timezone.utc)
    jst_dt = utc_dt.astimezone(jst)
    message_to_edit.send_at_jst = jst_dt
    
    session.close()
    return render_template('edit_scheduled.html', message=message_to_edit)

@app.route("/delete-scheduled/<int:msg_id>", methods=['POST'])
@auth_required
def delete_scheduled(msg_id):
    session = Session()
    message_to_delete = session.query(ScheduledMessage).filter_by(id=msg_id).first()
    user_id_for_redirect = message_to_delete.user_id if message_to_delete else None
    if message_to_delete:
        session.delete(message_to_delete)
        session.commit()
    session.close()
    if user_id_for_redirect:
        return redirect(url_for('admin_chat_detail_page', user_id=user_id_for_redirect))
    return redirect(url_for('admin_chat_page'))

@app.route("/edit-broadcast/<int:broadcast_id>", methods=['GET', 'POST'])
@auth_required
def edit_broadcast_page(broadcast_id):
    session = Session()
    broadcast_to_edit = session.query(ScheduledBroadcast).filter_by(id=broadcast_id).first()
    if not broadcast_to_edit:
        session.close()
        return "予約配信が見つかりません。", 404

    jst = timezone(timedelta(hours=9))
    if request.method == 'POST':
        broadcast_to_edit.name = request.form.get('name')
        send_at_str = request.form.get('send_at')
        if send_at_str:
            try:
                naive_dt = datetime.strptime(send_at_str, '%Y-%m-%d %H:%M')
                jst_dt = naive_dt.replace(tzinfo=jst)
                utc_dt = jst_dt.astimezone(timezone.utc)
                broadcast_to_edit.send_at = utc_dt
            except ValueError:
                return "日時の形式が正しくありません。", 400
        
        messages_info = json.loads(broadcast_to_edit.messages_info)
        new_text = request.form.get('message_text')
        if 'text_contents' in messages_info and new_text is not None:
            if messages_info['text_contents']:
                messages_info['text_contents'][0] = new_text
            else:
                 messages_info['text_contents'].append(new_text)
            broadcast_to_edit.messages_info = json.dumps(messages_info)

        session.commit()
        session.close()
        return jsonify({'status': 'success'})

    broadcast_to_edit.send_at_jst = broadcast_to_edit.send_at.astimezone(jst)
    
    try:
        messages_info = json.loads(broadcast_to_edit.messages_info)
        if messages_info.get('message_types') and messages_info.get('text_contents'):
            broadcast_to_edit.message_text = messages_info.get('text_contents')[0]
        else:
            broadcast_to_edit.message_text = "(画像やカルーセルを含むため、編集できません)"
    except (json.JSONDecodeError, IndexError):
        broadcast_to_edit.message_text = ""

    session.close()
    return render_template('edit_broadcast.html', broadcast=broadcast_to_edit)

@app.route("/delete-broadcast/<int:broadcast_id>", methods=['POST'])
@auth_required
def delete_broadcast(broadcast_id):
    session = Session()
    broadcast_to_delete = session.query(ScheduledBroadcast).filter_by(id=broadcast_id).first()
    if broadcast_to_delete:
        session.delete(broadcast_to_delete)
        session.commit()
    session.close()
    return redirect(url_for('admin_messaging_page'))

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

def build_messages_from_form(request_form, request_files):
    messages_to_send = []
    message_types = request_form.getlist('message_type')
    text_contents = request_form.getlist('text_content')
    uploaded_files = request_files
    imagemap_alt_texts = request_form.getlist('imagemap_alt_text')
    imagemap_action_types = request_form.getlist('imagemap_action_type')
    imagemap_action_data = request_form.getlist('imagemap_action_data')
    button_titles = request_form.getlist('button_title')
    button_texts = request_form.getlist('button_text')
    carousel_alt_texts = request_form.getlist('carousel_alt_text')
    action_message_indices = request_form.getlist('action_message_index', type=int)
    action_labels = request_form.getlist('action_label')
    action_types = request_form.getlist('action_type')
    action_data = request_form.getlist('action_data')
    column_message_indices = request_form.getlist('column_message_index', type=int)
    column_titles = request_form.getlist('column_title')
    column_texts = request_form.getlist('column_text')
    column_image_urls = request_form.getlist('column_image_url')
    action_column_indices = request_form.getlist('action_column_index', type=int)
    text_idx, file_idx, button_idx, carousel_idx, imagemap_idx, action_idx, column_idx = 0, 0, 0, 0, 0, 0, 0
    for i, msg_type in enumerate(message_types):
        if len(messages_to_send) >= 5: break
        if msg_type == 'text':
            if text_idx < len(text_contents) and text_contents[text_idx]:
                messages_to_send.append(TextSendMessage(text=text_contents[text_idx]))
            text_idx += 1
        elif msg_type == 'image' or msg_type == 'imagemap':
            file_key = f'image_file_{i}'
            if file_key in request_files:
                file = request_files[file_key]
                if file and allowed_file(file.filename):
                    filename = secure_filename(f"{uuid4().hex}.png")
                    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                    file.save(filepath)
                    image_url = url_for('uploaded_file', filename=filename, _external=True)
                    if '127.0.0.1' in image_url or 'localhost' in image_url:
                        ngrok_url = os.environ.get('NGROK_URL')
                        if ngrok_url:
                            image_url = ngrok_url + url_for('uploaded_file', filename=filename)
                        else:
                             image_url = image_url.replace('http://', 'https://')
                    
                    if msg_type == 'image':
                        messages_to_send.append(ImageSendMessage(original_content_url=image_url, preview_image_url=image_url))
                    else:
                        alt_text = imagemap_alt_texts[imagemap_idx] if imagemap_idx < len(imagemap_alt_texts) else "画像メッセージ"
                        action_type = imagemap_action_types[imagemap_idx] if imagemap_idx < len(imagemap_action_types) else 'message'
                        action_data = imagemap_action_data[imagemap_idx] if imagemap_idx < len(imagemap_action_data) else ''
                        action = None
                        area = ImagemapArea(x=0, y=0, width=1040, height=1040)
                        if action_type == 'uri' and action_data.startswith('http'):
                            action = URIImagemapAction(link_uri=action_data, area=area)
                        elif action_type == 'message' and action_data:
                             action = MessageImagemapAction(text=action_data, area=area)
                        if action:
                            message = ImagemapSendMessage(base_url=image_url, alt_text=alt_text, base_size=BaseSize(height=1040, width=1040), actions=[action])
                            messages_to_send.append(message)
                        imagemap_idx += 1
            file_idx += 1
        elif msg_type == 'button':
            actions = []
            num_actions = action_message_indices.count(i)
            for _ in range(num_actions):
                 if len(actions) >= 4: break
                 if action_idx < len(action_labels):
                    label = action_labels[action_idx]
                    action_type = action_types[action_idx]
                    data = action_data[action_idx]
                    if action_type == 'uri' and data.startswith('http'): actions.append(URIAction(label=label, uri=data))
                    elif action_type == 'message': actions.append(MessageAction(label=label, text=data))
                 action_idx += 1
            if actions:
                template = ButtonsTemplate(
                    title=button_titles[button_idx] if button_idx < len(button_titles) and button_titles[button_idx] else None,
                    text=button_texts[button_idx] if button_idx < len(button_texts) else " ",
                    actions=actions
                )
                messages_to_send.append(TemplateSendMessage(alt_text='ボタンメッセージ', template=template))
            button_idx += 1
        elif msg_type == 'carousel':
            columns = []
            num_columns = column_message_indices.count(i)
            for _ in range(num_columns):
                if len(columns) >= 10: break
                actions = []
                num_actions = action_column_indices.count(column_idx)
                for _ in range(num_actions):
                    if len(actions) >= 3: break
                    if action_idx < len(action_labels):
                        label = action_labels[action_idx]
                        action_type = action_types[action_idx]
                        data = action_data[action_idx]
                        if action_type == 'uri' and data.startswith('http'): actions.append(URIAction(label=label, uri=data))
                        elif action_type == 'message': actions.append(MessageAction(label=label, text=data))
                    action_idx += 1
                column = CarouselColumn(
                    thumbnail_image_url=column_image_urls[column_idx] if column_idx < len(column_image_urls) and column_image_urls[column_idx] else None,
                    title=column_titles[column_idx] if column_idx < len(column_titles) else None,
                    text=column_texts[column_idx] if column_idx < len(column_texts) else " ",
                    actions=actions
                )
                columns.append(column)
                column_idx += 1
            if columns:
                alt_text = carousel_alt_texts[carousel_idx] if carousel_idx < len(carousel_alt_texts) and carousel_alt_texts[carousel_idx] else "カルーセル"
                template = CarouselTemplate(columns=columns)
                messages_to_send.append(TemplateSendMessage(alt_text=alt_text, template=template))
            carousel_idx += 1
    return messages_to_send

@app.route("/send-message-from-admin", methods=['POST'])
@auth_required
def send_message_from_admin():
    line_bot_api = get_line_bot_api()
    if not line_bot_api: return "アクセストークンが設定されていません。", 500
    targeting_type = request.form.get('targeting_type')
    include_tags = request.form.getlist('include_tags')
    exclude_tags = request.form.getlist('exclude_tags')
    session = Session()
    query = session.query(User)
    if targeting_type == 'segmented' and (include_tags or exclude_tags):
        if include_tags: query = query.filter(and_(*[User.tags.like(f'%{tag}%') for tag in include_tags]))
        if exclude_tags: query = query.filter(and_(*[User.tags.notlike(f'%{tag}%') for tag in exclude_tags]))
    users = query.all()
    user_ids = [user.id for user in users]
    session.close()
    if not user_ids: return redirect(url_for('admin_messaging_page'))
    messages_to_send = build_messages_from_form(request.form, request.files)
    if user_ids and messages_to_send:
        try:
            for i in range(0, len(user_ids), 150):
                line_bot_api.multicast(user_ids[i:i+150], messages_to_send)
        except LineBotApiError as e:
            print(f"!!! 配信でエラー: {e}")
    return redirect(url_for('admin_messaging_page'))

@app.route("/schedule-message-from-admin", methods=['POST'])
@auth_required
def schedule_broadcast_from_admin():
    messages_info = {'files': {}}
    for i, msg_type in enumerate(request.form.getlist('message_type')):
        if msg_type in ['image', 'imagemap']:
            file_key = f'image_file_{i}'
            if file_key in request.files:
                file = request.files[file_key]
                if file and allowed_file(file.filename):
                    filename = secure_filename(f"{uuid4().hex}.png")
                    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                    file.save(filepath)
                    messages_info['files'][file_key] = filename

    targeting_info = {
        'targeting_type': request.form.get('targeting_type'),
        'include_tags': request.form.getlist('include_tags'),
        'exclude_tags': request.form.getlist('exclude_tags'),
    }
    messages_info.update({key: request.form.getlist(key) for key in request.form if key != 'send_at'})
    
    send_at_str = request.form.get('send_at')
    name = request.form.get('broadcast_name', '無題の配信')
    if not send_at_str: return "予約日時が指定されていません。", 400
    try:
        naive_dt = datetime.strptime(send_at_str, '%Y-%m-%d %H:%M')
    except ValueError:
        return "日時の形式が正しくありません。", 400
    jst = timezone(timedelta(hours=9))
    jst_dt = naive_dt.replace(tzinfo=jst)
    utc_dt = jst_dt.astimezone(timezone.utc)
    session = Session()
    new_broadcast = ScheduledBroadcast(
        name=name,
        targeting_info=json.dumps(targeting_info),
        messages_info=json.dumps(messages_info),
        send_at=utc_dt,
        status='pending'
    )
    session.add(new_broadcast)
    session.commit()
    session.close()
    return redirect(url_for('admin_messaging_page'))

# --- LINE Bot本体の機能 ---
@app.route("/callback", methods=['POST'])
def callback():
    session = Session()
    channel_secret_setting = session.query(Setting).filter_by(key='line_channel_secret').first()
    session.close()
    if not channel_secret_setting or not channel_secret_setting.value:
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