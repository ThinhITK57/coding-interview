```
root@jira-minio:~/minio# ls
certs  docker-compose.yaml
root@jira-minio:~/minio# nano docker-compose.yaml 
root@jira-minio:~/minio# pwd
/root/minio
root@jira-minio:~/minio# docker-compose ps
Name               Command               State                                           Ports                                         
---------------------------------------------------------------------------------------------------------------------------------------
minio   /usr/bin/docker-entrypoint ...   Up      0.0.0.0:19000->9000/tcp,:::19000->9000/tcp, 0.0.0.0:19001->9001/tcp,:::19001->9001/tcp
root@jira-minio:~/minio# 

```

