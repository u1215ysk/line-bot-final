import os
import sys
from flask import Flask, request, abort

from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.exceptions import (
    InvalidSignatureError
)
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage, FollowEvent
)

import sqlalchemy
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, String, DateTime, func

print("--- プログラム開始 ---")

app = Flask(__name__)

# --- 環境変数から設定を取得 ---
print("--- 環境変数の読み込み開始 ---")
channel_access_token = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')
channel_secret = os.environ.get('LINE_CHANNEL_SECRET')
database_url = os.environ.get('DATABASE_URL')
print("--- 環境変数の読み込み完了 ---")

if not all([channel_access_token, channel_secret, database_url]):
    print("!!! エラー: 必要な環境変数が設定されていません。")
    sys.exit(1)

line_bot_api = LineBotApi(channel_access_token)
handler = WebhookHandler(channel_secret)

# --- データベースの設定 ---
Base = declarative_base()

# ユーザー情報を保存するためのテーブル定義
class User(Base):
    __tablename__ = 'users'
    id = Column(String, primary_key=True)
    created_at = Column(DateTime, server_default=func.now())

try:
    print("--- データベースエンジン作成開始 ---")
    engine = sqlalchemy.create_engine(database_url)
    print("--- データベースエンジン作成完了 ---")
    
    print("--- テーブル作成処理開始 ---")
    Base.metadata.create_all(engine) # テーブルがなければ作成
    print("--- テーブル作成処理完了 ---")

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

# --- イベントハンドラーの定義 ---
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
    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text=event.message.text))

@app.route("/", methods=['GET'])
def health_check():
    return 'OK', 200

if __name__ == "__main__":
    print("--- Flaskサーバー起動準備 ---")
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)