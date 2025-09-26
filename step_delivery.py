import os
import sys
import time
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

from linebot import (
    LineBotApi
)
from linebot.exceptions import (
    LineBotApiError
)
from linebot.models import (
    TextSendMessage
)

from sqlalchemy import create_engine, Column, String, DateTime, func, Integer, Text
from sqlalchemy.orm import sessionmaker, declarative_base

# .envファイルをロード
load_dotenv()

print("--- ステップ配信・予約投稿バッチ開始 ---")

# --- 環境変数から設定を取得 ---
channel_access_token = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')
db_url_from_env = os.environ.get('DATABASE_URL')

if db_url_from_env and db_url_from_env.startswith("postgres://"):
    database_url = db_url_from_env.replace("postgres://", "postgresql+psycopg://", 1)
else:
    database_url = db_url_from_env

if not all([channel_access_token, database_url]):
    print("!!! エラー: 必要な環境変数が設定されていません。")
    sys.exit(1)

line_bot_api = LineBotApi(channel_access_token)

# --- データベースのモデル定義 (main.pyと完全に合わせる) ---
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

class BatchRunLog(Base):
    __tablename__ = 'batch_run_log'
    id = Column(Integer, primary_key=True)
    last_step_check_date = Column(DateTime, nullable=False)

try:
    engine = create_engine(database_url, pool_pre_ping=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
except Exception as e:
    print(f"!!! データベース接続でエラー: {e}")
    sys.exit(1)

# --- 配信ロジック ---
def process_step_messages(session, line_bot_api):
    print("--- ステップ配信のチェック開始 ---")
    today = datetime.now(timezone.utc).date()
    
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
        log.last_step_check_date = datetime.now(timezone.utc)
    else:
        new_log = BatchRunLog(last_step_check_date=datetime.now(timezone.utc))
        session.add(new_log)
    session.commit()

def process_scheduled_messages(session, line_bot_api):
    print("--- 予約投稿のチェック開始 ---")
    now = datetime.now(timezone.utc)
    
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
            
            history_message = Message(
                user_id=msg.user_id,
                sender_type='admin',
                content=msg.message_text,
                created_at=now
            )
            session.add(history_message)
            
            msg.status = 'sent'
        except LineBotApiError as e:
            print(f"!!! 予約投稿(ID: {msg.id})の送信でエラー: {e}")
            msg.status = 'error'
    
    session.commit()
    print(f"{len(messages_to_send)}件の予約投稿を処理しました。")

def main_loop():
    while True:
        print(f"\n--- {datetime.now()} バッチ処理を開始 ---")
        session = Session()
        try:
            process_step_messages(session, line_bot_api)
            process_scheduled_messages(session, line_bot_api)
        except Exception as e:
            print(f"!!! バッチ処理中に予期せぬエラーが発生: {e}")
            session.rollback()
        finally:
            session.close()
            print("--- バッチ処理終了 ---")
        
        print("--- 60秒待機しています... ---")
        time.sleep(60)

if __name__ == "__main__":
    main_loop()