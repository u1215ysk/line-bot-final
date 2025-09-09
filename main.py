import os
import sys
from flask import Flask, request, abort

from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.exceptions import (
    InvalidSignatureError, LineBotApiError
)
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage, FollowEvent
)

from sqlalchemy import create_engine, Column, String, DateTime, func
from sqlalchemy.orm import sessionmaker, declarative_base

print("--- プログラム開始 ---")

app = Flask(__name__)

# --- 環境変数から設定を取得 ---
print("--- 環境変数の読み込み開始 ---")
channel_access_token = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')
channel_secret = os.environ.get('LINE_CHANNEL_SECRET')

# KoyebのデータベースURLをSQLAlchemyが理解できる形式(postgresql+psycopg://)に変換
db_url = os.environ.get('DATABASE_URL')
if db_url and db_url.startswith("postgres://"):
    database_url = db_url.replace("postgres://", "postgresql+psycopg://", 1)
else:
    database_url = db_url
print("--- 環境変数の読み込み完了 ---")

if not all([channel_access_token, channel_secret, database_url]):
    print("!!! エラー: 必要な環境変数が設定されていません。")
    sys.exit(1)

line_bot_api = LineBotApi(channel_access_token)
handler = WebhookHandler(channel_secret)

# --- データベースの設定 ---
Base = declarative_base()

# ▼ ユーザーテーブルにtags列を追加 ▼
class User(Base):
    __tablename__ = 'users'
    id = Column(String, primary_key=True)
    tags = Column(String, default="") # タグを保存するための列
    created_at = Column(DateTime, server_default=func.now())

try:
    print("--- データベースエンジン作成開始 ---")
    engine = create_engine(database_url)
    print("--- データベースエンジン作成完了 ---")
    
    print("--- テーブル作成/更新処理開始 ---")
    Base.metadata.create_all(engine) # テーブル構造の変更を反映
    print("--- テーブル作成/更新処理完了 ---")
    Session = sessionmaker(bind=engine)
except Exception as e:
    print(f"!!! データベース接続またはテーブル作成でエラー: {e}")
    sys.exit(1)
# -------------------------

@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'

# --- ▼ セグメント配信（クーポン希望者向け）▼ ---
@app.route("/push-coupon", methods=['GET'])
def push_to_coupon_users():
    session = Session()
    # 'coupon'タグを持つユーザーだけを抽出
    coupon_users = session.query(User).filter(User.tags.like('%coupon%')).all()
    user_ids = [user.id for user in coupon_users]
    session.close()

    if not user_ids:
        return "メッセージを送るクーポン希望者がいません。"

    try:
        line_bot_api.multicast(
            user_ids,
            TextSendMessage(text="クーポンをご希望の方だけに、特別なメッセージをお送りしています！")
        )
        return f"メッセージを{len(user_ids)}人のクーポン希望者に送信しました。"
    except LineBotApiError as e:
        print(f"!!! メッセージ送信でエラー: {e}")
        return "メッセージの送信に失敗しました。", 500
# --- ▲ セグメント配信 ▲ ---

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

# ▼ メッセージ受信時の処理を改造 ▼
@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_id = event.source.user_id
    user_message = event.message.text

    # 「クーポン」というメッセージに反応してタグを付ける
    if user_message == "クーポン":
        session = Session()
        user = session.query(User).filter_by(id=user_id).first()
        if user and "coupon" not in user.tags:
            user.tags += "coupon," # タグを追加
            session.commit()
            reply_text = "クーポン希望者として登録しました！"
        else:
            reply_text = "すでに登録済みです。"
        session.close()
        
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=reply_text)
        )
    else:
        # 通常のオウム返し
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=user_message)
        )

@app.route("/", methods=['GET'])
def health_check():
    return 'OK', 200

if __name__ == "__main__":
    print("--- Flaskサーバー起動準備 ---")
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)