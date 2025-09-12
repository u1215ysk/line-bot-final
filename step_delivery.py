import os
import sys
from datetime import datetime, timedelta

from linebot import LineBotApi
from linebot.models import TextSendMessage
from linebot.exceptions import LineBotApiError

from sqlalchemy import create_engine, Column, String, DateTime, func, Integer, Text
from sqlalchemy.orm import sessionmaker, declarative_base

print("--- ステップ配信・予約投稿バッチ開始 ---")

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

# --- データベースのモデル定義 ---
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

class ScheduledMessage(Base):
    __tablename__ = 'scheduled_messages'
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String, nullable=False)
    message_text = Column(Text, nullable=False)
    send_at = Column(DateTime, nullable=False)
    status = Column(String, default='pending')

# ▼▼▼ 最後にステップ配信をチェックした日を記録するテーブル ▼▼▼
class BatchRunLog(Base):
    __tablename__ = 'batch_run_log'
    id = Column(Integer, primary_key=True)
    last_step_check_date = Column(DateTime, nullable=False)

try:
    engine = create_engine(database_url, pool_pre_ping=True)
    Base.metadata.create_all(engine) # テーブルを自動作成
    Session = sessionmaker(bind=engine)
    session = Session()
    print("--- データベース接続完了 ---")
except Exception as e:
    print(f"!!! データベース接続でエラー: {e}")
    sys.exit(1)

# --- 配信ロジック ---
def process_step_messages(session, line_bot_api):
    print("--- ステップ配信のチェック開始 ---")
    today = datetime.utcnow().date()
    
    # 最後にチェックした日を取得
    log = session.query(BatchRunLog).first()
    if log and log.last_step_check_date.date() == today:
        print("本日のステップ配信は既にチェック済みです。")
        return # 今日の日付で既に実行済みの場合はスキップ
    
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

    # 最終チェック日を今日の日付で更新または新規作成
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

def main():
    # ステップ配信は1日に1回だけ実行されるようにチェック
    process_step_messages(session, line_bot_api)
    # 予約投稿は毎分チェック
    process_scheduled_messages(session, line_bot_api)
    
    session.close()
    print("--- バッチ処理終了 ---")

if __name__ == "__main__":
    main()