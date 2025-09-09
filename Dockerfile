# 1. ベースとなる公式Pythonイメージを指定
FROM python:3.11-slim

# 2. 作業ディレクトリを設定
WORKDIR /app

# 3. requirements.txtをコピーして、ライブラリをインストール
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. プロジェクトのファイルを全てコピー
COPY . .

# 5. Webサービス用の起動コマンド
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "main:app"]