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
    display_name = Column(String) # ▼ 追加 ▼
    nickname = Column(String)     # ▼ 追加 ▼    
    tags = Column(String, default="")
    sent_steps = Column(String, default="") # 送信済みステップを記録 (例: "1,3,")
    created_at = Column(DateTime, server_default=func.now())

class StepMessage(Base):
    __tablename__ = 'step_messages'
    id = Column(Integer, primary_key=True, autoincrement=True)
    days_after = Column(Integer, nullable=False)
    message_text = Column(Text, nullable=False)

class Message(Base):
    __tablename__ = 'messages'
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String, nullable=False)
    sender_type = Column(String, nullable=False)  # 'user' or 'admin'
    content = Column(Text, nullable=False)
    created_at = Column(DateTime, server_default=func.now())

try:
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    print("--- データベース接続完了 ---")
except Exception as e:
    print(f"!!! データベース接続でエラー: {e}")
    sys.exit(1)

# --- ▼▼▼ 配信ロジックを「送信済みチェック機能付き」にアップグレード ▼▼▼ ---
def main():
    today = datetime.utcnow().date()
    scenarios = session.query(StepMessage).all()
    print(f"{len(scenarios)}件のシナリオをデータベースから取得しました。")

    for scenario in scenarios:
        target_date = today - timedelta(days=scenario.days_after)
        target_users = session.query(User).filter(func.date(User.created_at) == target_date).all()
        
        if not target_users:
            continue

        users_to_send = []
        # 送信対象の中から、まだこのステップを送られていない人だけを絞り込む
        for user in target_users:
            sent_steps_list = user.sent_steps.split(',')
            if str(scenario.days_after) not in sent_steps_list:
                users_to_send.append(user)
        
        if users_to_send:
            user_ids_to_send = [user.id for user in users_to_send]
            try:
                print(f"登録{scenario.days_after}日後の{len(user_ids_to_send)}人にメッセージを送信します...")
                line_bot_api.multicast(user_ids_to_send, TextSendMessage(text=scenario.message_text))
                
                # 送信成功後、データベースに「送信済み」の記録を付ける
                for user in users_to_send:
                    user.sent_steps += f"{scenario.days_after},"
                session.commit()
                print("送信記録をデータベースに保存しました。")

            except LineBotApiError as e:
                print(f"!!! シナリオ(ID: {scenario.id})のメッセージ送信でエラー: {e}")
                session.rollback() # 送信に失敗した場合は、記録を付けない

    session.close()
    print("--- ステップ配信バッチ終了 ---")

if __name__ == "__main__":
    main()