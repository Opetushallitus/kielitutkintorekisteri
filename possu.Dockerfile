FROM postgres:19
RUN localedef -i fi_FI -c -f UTF-8 -A /usr/share/locale/locale.alias fi_FI.UTF-8
