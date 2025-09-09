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

# --- ▼▼▼ 管理画面用のコード ▼▼▼ ---
@app.route("/admin")
def admin_page():
    session = Session()
    all_users = session.query(User).order_by(User.created_at.desc()).all()
    session.close()
    # templates/admin.html を表示し、ユーザーリストを渡す
    return render_template('admin.html', users=all_users)

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
            line_bot_api.multicast(
                user_ids,
                TextSendMessage(text=message_text)
            )
        except LineBotApiError as e:
            print(f"!!! 一斉配信でエラー: {e}")

    return redirect(url_for('admin_page'))
# --- ▲▲▲ 管理画面用のコード ▲▲▲ ---


@app.route("/callback", methods=['POST'])
def callback():
    # (省略...この部分は変更ありません)
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'

# (省略... /push-coupon, /push-message, handle_follow, handle_message, health_check のコードは変更ありません)
# (そのまま最新のコードをお使いください)
# ...
# ...

@app.route("/push-coupon", methods=['GET'])
def push_to_coupon_users():
    session = Session()
    coupon_users = session.query(User).filter(User.tags.like('%coupon%')).all()
    user_ids = [user.id for user in coupon_users]
    session.close()
    if not user_ids: return "メッセージを送るクーポン希望者がいません。"
    try:
        line_bot_api.multicast(user_ids, TextSendMessage(text="クーポンをご希望の方だけに、特別なメッセージをお送りしています！"))
        return f"メッセージを{len(user_ids)}人のクーポン希望者に送信しました。"
    except LineBotApiError as e:
        print(f"!!! メッセージ送信でエラー: {e}")
        return "メッセージの送信に失敗しました。", 500

@app.route("/push-message", methods=['GET'])
def push_to_all_users():
    session = Session()
    all_users = session.query(User).all()
    user_ids = [user.id for user in all_users]
    session.close()
    if not user_ids: return "メッセージを送るユーザーがいません。"
    try:
        line_bot_api.multicast(user_ids, TextSendMessage(text="これは管理者からのテスト配信です。"))
        return f"メッセージを{len(user_ids)}人のユーザーに送信しました。"
    except LineBotApiError as e:
        print(f"!!! メッセージ送信でエラー: {e}")
        return "メッセージの送信に失敗しました。", 500

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
        reply_message = TextSendMessage(
            text="サービスに満足していますか？",
            quick_reply=quick_reply_buttons
        )
        line_bot_api.reply_message(event.token, reply_message)

    elif user_message == "はい":
        if user and "satisfied" not in user.tags:
            user.tags += "satisfied,"
            session.commit()
            reply_text = "ありがとうございます！ご回答を記録しました。"
        else:
            reply_text = "ご回答ありがとうございます！"
        line_bot_api.reply_message(event.token, TextSendMessage(text=reply_text))

    elif user_message == "いいえ":
        if user and "unsatisfied" not in user.tags:
            user.tags += "unsatisfied,"
            session.commit()
            reply_text = "ご意見ありがとうございます。今後の参考にさせていただきます。"
        else:
            reply_text = "ご意見ありがとうございます。"
        line_bot_api.reply_message(event.token, TextSendMessage(text=reply_text))
        
    elif user_message == "クーポン":
        if user and "coupon" not in user.tags:
            user.tags += "coupon,"
            session.commit()
            reply_text = "クーポン希望者として登録しました！"
        else:
            reply_text = "すでに登録済みです。"
        line_bot_api.reply_message(event.token, TextSendMessage(text=reply_text))

    else:
        # 通常のオウム返し
        line_bot_api.reply_message(
            event.token,
            TextSendMessage(text=user_message)
        )
    
    session.close()

@app.route("/", methods=['GET'])
def health_check():
    return 'OK', 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)