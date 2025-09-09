import os
import sys
from datetime import datetime, timedelta

from linebot import LineBotApi
from linebot.models import TextSendMessage
from linebot.exceptions import LineBotApiError

from sqlalchemy import create_engine, Column, String, DateTime, func
from sqlalchemy.orm import sessionmaker, declarative_base

print("--- ステップ配信バッチ開始 ---")

# --- 環境変数から設定を取得 ---
channel_access_token = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')
database_url_from_env = os.environ.get('DATABASE_URL')

if not all([channel_access_token, database_url_from_env]):
    print("!!! エラー: 必要な環境変数が設定されていません。")
    sys.exit(1)
    
# DB URLの変換
if database_url_from_env.startswith("postgres://"):
    database_url = database_url_from_env.replace("postgres://", "postgresql+psycopg://", 1)
else:
    database_url = database_url_from_env

line_bot_api = LineBotApi(channel_access_token)

# --- データベースの設定 ---
Base = declarative_base()
class User(Base):
    __tablename__ = 'users'
    id = Column(String, primary_key=True)
    tags = Column(String, default="")
    created_at = Column(DateTime, server_default=func.now())

try:
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    print("--- データベース接続完了 ---")
except Exception as e:
    print(f"!!! データベース接続でエラー: {e}")
    sys.exit(1)

# --- 配信ロジック ---
def main():
    today = datetime.utcnow().date()
    
    # --- 登録翌日のユーザーに配信 ---
    target_date_1 = today - timedelta(days=1)
    users_day_1 = session.query(User).filter(func.date(User.created_at) == target_date_1).all()
    if users_day_1:
        user_ids_1 = [user.id for user in users_day_1]
        try:
            print(f"{len(user_ids_1)}人の登録翌日ユーザーに送信します。")
            line_bot_api.multicast(user_ids_1, TextSendMessage(text="〇〇さん（アカウント名）です！これからよろしくお願いします！"))
        except LineBotApiError as e:
            print(f"!!! 登録翌日のメッセージ送信でエラー: {e}")

    # --- 登録3日後のユーザーに配信 ---
    target_date_3 = today - timedelta(days=3)
    users_day_3 = session.query(User).filter(func.date(User.created_at) == target_date_3).all()
    if users_day_3:
        user_ids_3 = [user.id for user in users_day_3]
        try:
            print(f"{len(user_ids_3)}人の登録3日後ユーザーに送信します。")
            line_bot_api.multicast(user_ids_3, TextSendMessage(text="使い方には慣れましたか？何か分からないことがあれば、いつでもメッセージを送ってくださいね！"))
        except LineBotApiError as e:
            print(f"!!! 登録3日後のメッセージ送信でエラー: {e}")

    session.close()
    print("--- ステップ配信バッチ終了 ---")

if __name__ == "__main__":
    main()