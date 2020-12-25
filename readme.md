```bash
curl -i 'http://localhost:8080/task'

curl -i -X PUT 'http://localhost:8080/task?afterSeconds=5&data=100500'
curl -i -X PUT 'http://localhost:8080/task?afterSeconds=5&data=100501'
```

Open the database
```
docker-compose exec postgresql psql -U scheduler
```