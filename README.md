# Запуск

```bash
docker-compose up --build -d
```
## Остановка

```bash
docker-compose down
```

# Отчеты в Datalens (если не грузится - перезагрузите страницу)
[Datalens](https://datalens.yandex/oz5u7s2o0e9u9)



# Скрипт с анализом данных
[analys.sql](analys.sql)


# Импорт данных в постгре
[import](init/01-import-data.sql)

# Скрипт по заполнению данных по модели снежинка
[ETL_snowflake.py](notebooks/ETL_snowflake.py)

# Скрипт по записи отчетов в БД
[ETL_report_all.py](notebooks/ETL_report_all.py)