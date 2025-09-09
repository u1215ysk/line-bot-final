import os
import sys
from flask import Flask, request, abort, render_template, redirect, url_for

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

from sqlalchemy import create_engine, Column, String, DateTime, func
from sqlalchemy.orm import sessionmaker, declarative_base

app = Flask(__name__)

# --- 環境変数から設定を取得 ---
channel_access_token = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')
channel_secret = os.environ.get('LINE_CHANNEL_SECRET')

db_url = os.environ.get('DATABASE_URL')
if db_url and db_url.startswith("postgres://"):
    database_url = db_url.replace("postgres://", "postgresql+psycopg://", 1)
else:
    database_url = db_url

if not all([channel_access_token, channel_secret, database_url]):
    print("!!! エラー: 必要な環境変数が設定されていません。")
    sys.exit(1)

line_bot_api = LineBotApi(channel_access_token)
handler = WebhookHandler(channel_secret)

# --- データベースの設定 ---
Base = declarative_base()
class User(Base):
    __tablename__ = 'users'
    id = Column(String, primary_key=True)
    tags = Column(String, default="")
    created_at = Column(DateTime, server_default=func.now())

try:
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
except Exception as e:
    print(f"!!! データベース接続またはテーブル作成でエラー: {e}")
    sys.exit(1)
# -------------------------

# --- ▼▼▼ 管理画面用のコード（編集機能を追加）▼▼▼ ---
@app.route("/admin")
def admin_page():
    session = Session()
    all_users = session.query(User).order_by(User.created_at.desc()).all()
    session.close()
    return render_template('admin.html', users=all_users)

@app.route("/edit-user/<user_id>")
def edit_user_page(user_id):
    session = Session()
    user = session.query(User).filter_by(id=user_id).first()
    session.close()
    if not user:
        return "ユーザーが見つかりません。", 404
    return render_template('edit_user.html', user=user)

@app.route("/update-user-tags/<user_id>", methods=['POST'])
def update_user_tags(user_id):
    new_tags = request.form['tags']
    session = Session()
    user = session.query(User).filter_by(id=user_id).first()
    if user:
        user.tags = new_tags
        session.commit()
    session.close()
    return redirect(url_for('admin_page'))

@app.route("/send-broadcast", methods=['POST'])
def send_broadcast():
    message_text = request.form['message']
    if not message_text:
        return redirect(url_for('admin_page'))
    session = Session()
    all_users = session.query(User).all()
    user_ids = [user.id for user in all_users]
    session.close()
    if user_ids:
        try:
            line_bot_api.multicast(user_ids, TextSendMessage(text=message_text))
        except LineBotApiError as e:
            print(f"!!! 一斉配信でエラー: {e}")
    return redirect(url_for('admin_page'))

@app.route("/send-segmented", methods=['POST'])
def send_segmented():
    tag = request.form['tag']
    message_text = request.form['message']
    if not tag or not message_text:
        return redirect(url_for('admin_page'))
    session = Session()
    tagged_users = session.query(User).filter(User.tags.like(f'%{tag}%')).all()
    user_ids = [user.id for user in tagged_users]
    session.close()
    if user_ids:
        try:
            line_bot_api.multicast(user_ids, TextSendMessage(text=message_text))
        except LineBotApiError as e:
            print(f"!!! セグメント配信でエラー: {e}")
    return redirect(url_for('admin_page'))
# --- ▲▲▲ 管理画面用のコード ▲▲▲ ---


@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'


@handler.add(FollowEvent)
def handle_follow(event):
    session = Session()
    user_id = event.source.user_id
    existing_user = session.query(User).filter_by(id=user_id).first()
    if not existing_user:
        new_user = User(id=user_id)
        session.add(new_user)
        session.commit()
        print(f"新しいユーザーが追加されました: {user_id}")
    session.close()

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_id = event.source.user_id
    user_message = event.message.text
    session = Session()
    user = session.query(User).filter_by(id=user_id).first()
    
    if user_message == "アンケート":
        quick_reply_buttons = QuickReply(items=[
            QuickReplyButton(action=MessageAction(label="はい", text="はい")),
            QuickReplyButton(action=MessageAction(label="いいえ", text="いいえ")),
        ])
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

    else:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=user_message))
    
    session.close()

@app.route("/", methods=['GET'])
def health_check():
    return 'OK', 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)