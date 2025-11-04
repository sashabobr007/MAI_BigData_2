# Запуск

```bash
docker-compose up --build -d
```
## Остановка

```bash
docker-compose down
```

# Отчеты в Datalens 
### (если не грузится - перезагрузите страницу, или посмотрите видео - clickhouse лежит на слабой ВМ)
[Datalens](https://datalens.yandex/oz5u7s2o0e9u9)

[Datalens_video](https://disk.yandex.ru/i/DaRDpp4Z4h9Ybw)


# Скрипт с анализом данных
[analys.sql](analys.sql)


# Импорт данных в постгре
[import](init/01-import-data.sql)

# Скрипт по заполнению данных по модели снежинка
[ETL_snowflake.py](notebooks/ETL_snowflake.py)

# Скрипт по записи отчетов в БД
[ETL_report_all.py](notebooks/ETL_report_all.py)