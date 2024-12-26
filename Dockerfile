FROM python:3.12-slim

WORKDIR /app

# Install cron and required packages
RUN apt-get update && apt-get install -y cron

# Copy files
COPY requirements.txt .
COPY fetch_puzzle_stats.py .
COPY update_crossword_stats.py .
COPY .env .

# Install Python dependencies
RUN pip install pandas requests python-dotenv tqdm sqlalchemy psycopg2-binary 

# Setup cron
RUN echo "0 */12 * * * /usr/local/bin/python /app/update_crossword_stats.py --days-back 365 >> /app/cron.log 2>&1" > /etc/cron.d/crossword-cron
RUN chmod 0644 /etc/cron.d/crossword-cron
RUN crontab /etc/cron.d/crossword-cron

VOLUME /app/data

CMD ["cron", "-f"]
