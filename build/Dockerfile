FROM iregistry.baidu-int.com/acg-aibp/base-image/aibp-alpine-3.18:gcc-8-runtime-1.0.2

ENV TIME_ZONE=Asia/Shanghai
ENV WORK_DIR /home/work

VOLUME $WORK_DIR/session_watcher/log
COPY output/ $WORK_DIR/session_watcher/

WORKDIR $WORK_DIR/session_watcher

RUN [ -d "build" ] && { cp -r "build"/* . && rm -rf "build";} || echo "build目录不存在。"
RUN ln -snf /usr/share/zoneinfo/$TIME_ZONE /etc/localtime && echo $TIME_ZONE > /etc/timezone
RUN chown -R bml:bml $WORK_DIR && \
    chmod +x $WORK_DIR/session_watcher/bin/session_watcher && \
    rm -rf arm64_build

USER bml
