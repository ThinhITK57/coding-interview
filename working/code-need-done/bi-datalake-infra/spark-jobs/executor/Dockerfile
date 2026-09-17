FROM spark-client-base-image:latest

COPY run_cron_etls.sh /run_cron_etls.sh
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /run_cron_etls.sh
RUN ln -s /entrypoint.sh /usr/local/bin/entrypoint.sh

RUN useradd -ms /bin/bash admin


WORKDIR /workspace

ENTRYPOINT ["/entrypoint.sh"]