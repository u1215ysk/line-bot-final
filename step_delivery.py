import os
import sys
from datetime import datetime, timedelta

from linebot import LineBotApi
from linebot.models import TextSendMessage
from linebot.exceptions import LineBotApiError

from sqlalchemy import create_engine, Column, String, DateTime, func, Integer, Text
from sqlalchemy.orm import sessionmaker, declarative_base

print("--- ステップ配信バッチ開始 ---")

# --- 環境変数から設定を取得 ---
channel_access_token = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')
db_url = os.environ.get('DATABASE_URL')
if db_url and db_url.startswith("postgres://"):
    database_url = db_url.replace("postgres://", "postgresql+psycopg://", 1)
else:
    database_url = db_url

if not all([channel_access_token, database_url]):
    print("!!! エラー: 必要な環境変数が設定されていません。")
    sys.exit(1)

line_bot_api = LineBotApi(channel_access_token)

# --- データベースのモデル定義 (main.pyと合わせる) ---
Base = declarative_base()
class User(Base):
    __tablename__ = 'users'
    id = Column(String, primary_key=True)
    tags = Column(String, default="")
    created_at = Column(DateTime, server_default=func.now())

class StepMessage(Base):
    __tablename__ = 'step_messages'
    id = Column(Integer, primary_key=True, autoincrement=True)
    days_after = Column(Integer, nullable=False)
    message_text = Column(Text, nullable=False)

try:
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    print("--- データベース接続完了 ---")
except Exception as e:
    print(f"!!! データベース接続でエラー: {e}")
    sys.exit(1)

# --- ▼▼▼ 配信ロジックをデータベース参照型にアップグレード ▼▼▼ ---
def main():
    today = datetime.utcnow().date()
    
    # データベースから有効なシナリオを全て取得
    scenarios = session.query(StepMessage).all()
    print(f"{len(scenarios)}件のシナリオをデータベースから取得しました。")

    for scenario in scenarios:
        # シナリオで指定された日数に基づいて、ターゲットとなる登録日を計算
        target_date = today - timedelta(days=scenario.days_after)
        
        # 該当するユーザーを検索
        target_users = session.query(User).filter(func.date(User.created_at) == target_date).all()
        
        if target_users:
            user_ids = [user.id for user in target_users]
            try:
                print(f"登録{scenario.days_after}日後の{len(user_ids)}人にメッセージを送信します: {scenario.message_text}")
                line_bot_api.multicast(user_ids, TextSendMessage(text=scenario.message_text))
            except LineBotApiError as e:
                print(f"!!! シナリオ(ID: {scenario.id})のメッセージ送信でエラー: {e}")

    session.close()
    print("--- ステップ配信バッチ終了 ---")

if __name__ == "__main__":
    main()