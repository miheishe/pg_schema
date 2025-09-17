# pg\_ascii\_schema — ASCII/JSON-обзор структуры PostgreSQL

Минималистичный CLI-скрипт на Python, который подключается к БД по DSN `postgresql://…` и **потоково** печатает дерево структуры:

* **схемы**

  * **таблицы / представления / матвью / foreign tables**

    * **колонки** (тип, `NOT NULL`, `DEFAULT`, + имя встроенной функции из `DEFAULT`, если есть)
    * *(опц.)* **indexes**
    * *(опц.)* **foreign\_keys**: `outgoing` и `incoming`
    * *(опц.)* **triggers**
  * *(опц.)* **functions**: `name(args) -> return_type`

Вывод возможен в формате **ASCII** (дерево) или **JSON** (структура). Скрипт рассчитан на **очень большие БД (1 ТБ+)** — использует **server-side cursors** и не держит всё в ОЗУ.

---

## Установка

Зависимости: **Python 3.8+**, библиотека **psycopg v3**.

```bash
pip install "psycopg[binary]"
```

Сохраните файл скрипта как `pg_schema.py` и сделайте исполняемым:

```bash
chmod +x pg_schema.py
```

---

## Быстрый старт

ASCII в stdout:

```bash
./pg_schema.py --dsn postgresql://user:pass@host:5432/db --schema public
```

JSON в файл:

```bash
./pg_schema.py --dsn postgresql://user:pass@host:5432/db --schema public \
  --include-indexes --include-fkeys --include-triggers \
  --format json --pretty --output schema.json
```

---

## Формат ASCII (пример)

```
public
├─ users [table]
│  ├─ columns
│  │  ├─ id: bigint NOT NULL DEFAULT nextval('users_id_seq'::regclass) [func: nextval]
│  │  ├─ email: text NOT NULL
│  │  └─ created_at: timestamp with time zone DEFAULT now() [func: now]
│  ├─ indexes
│  │  ├─ users_pkey [PK] :: CREATE UNIQUE INDEX users_pkey ON public.users USING btree (id)
│  │  └─ users_email_key [UNIQ] :: CREATE UNIQUE INDEX users_email_key ON public.users USING btree (email)
│  ├─ foreign_keys
│  │  ├─ outgoing
│  │  │  └─ users_role_id_fkey -> roles :: FOREIGN KEY (role_id) REFERENCES roles(id)
│  │  └─ incoming
│  │     └─ sessions_user_id_fkey <- sessions :: FOREIGN KEY (user_id) REFERENCES users(id)
│  └─ triggers
│     └─ users_updated_at_set [func: set_timestamp] :: CREATE TRIGGER ...
└─ functions
   └─ set_timestamp() -> trigger
```

---

## Формат JSON (пример)

```json
{
  "schemas": [
    {
      "name": "public",
      "functions": [
        {"name": "set_timestamp", "args": "", "return_type": "trigger"}
      ],
      "relations": [
        {
          "name": "users",
          "kind": "[table]",
          "columns": [
            {"name": "id", "type": "bigint", "not_null": true, "default": "nextval('users_id_seq'::regclass)", "default_func": "nextval"},
            {"name": "email", "type": "text", "not_null": true},
            {"name": "created_at", "type": "timestamp with time zone", "default": "now()", "default_func": "now"}
          ],
          "indexes": [
            {"name": "users_pkey", "primary": true, "unique": true, "invalid": false, "definition": "CREATE UNIQUE INDEX users_pkey ON public.users USING btree (id)"}
          ],
          "foreign_keys": {
            "outgoing": [
              {"name": "users_role_id_fkey", "ref_table": "roles", "definition": "FOREIGN KEY (role_id) REFERENCES roles(id)"}
            ],
            "incoming": []
          },
          "triggers": [
            {"name": "users_updated_at_set", "function": "set_timestamp", "definition": "CREATE TRIGGER ..."}
          ]
        }
      ]
    }
  ]
}
```

---

## Параметры CLI

| Параметр                 |         Тип | Описание                                                                                                     |
| ------------------------ | ----------: | ------------------------------------------------------------------------------------------------------------ |
| `--dsn`                  | str (обяз.) | Строка подключения `postgresql://user:pass@host:port/dbname`. Можно использовать `DATABASE_URL`.             |
| `--schema`               |         str | Имя схемы **или** паттерн (если указан `--schema-regex`). Если не задано — обход всех пользовательских схем. |
| `--schema-regex`         |        флаг | Интерпретировать `--schema` как регулярное выражение (Python regex).                                         |
| `--include-views`        |        флаг | Добавить `VIEW` в обход.                                                                                     |
| `--include-matviews`     |        флаг | Добавить `MATERIALIZED VIEW`.                                                                                |
| `--include-foreign`      |        флаг | Добавить `FOREIGN TABLE`.                                                                                    |
| `--include-funcs`        |        флаг | Печатать функции/процедуры схем.                                                                             |
| `--include-all-schemas`  |        флаг | Включить системные схемы (`pg_*`, `information_schema`).                                                     |
| `--include-indexes`      |        флаг | Печатать индексы таблиц.                                                                                     |
| `--include-fkeys`        |        флаг | Печатать внешние ключи: `outgoing` и `incoming`.                                                             |
| `--include-triggers`     |        флаг | Печатать триггеры.                                                                                           |
| `--statement-timeout-ms` |         int | Установить `SET LOCAL statement_timeout` (мс). `0` — не задавать.                                            |
| `--application-name`     |         str | `application_name` соединения.                                                                               |
| `--output`               |         str | Путь к файлу для сохранения результата (по умолчанию stdout).                                                |
| `--format`               |  ascii/json | Формат вывода (`ascii` — дерево, `json` — структура).                                                        |
| `--pretty`               |        флаг | Красивый JSON с отступами.                                                                                   |

---

## Примеры

### ASCII в stdout

```bash
./pg_schema.py --dsn "$DATABASE_URL" --schema public
```

### ASCII в файл

```bash
./pg_schema.py --dsn "$DATABASE_URL" --schema public --output schema.txt
```

### JSON (компактный) в файл

```bash
./pg_schema.py --dsn "$DATABASE_URL" --schema public \
  --include-indexes --include-fkeys --include-triggers \
  --format json --output schema.json
```

### JSON (красивый) в stdout

```bash
./pg_schema.py --dsn "$DATABASE_URL" --format json --pretty
```

---

## Производительность и масштаб

* **Потоковая выборка**: server-side курсоры.
* **Точечные запросы** для деталей таблиц.
* **READ ONLY** транзакция и `statement_timeout` для безопасности.
* Быстрые системные функции `pg_get_*def`.

---

## Безопасность

* Соединение открывается в **read-only** транзакции.
* Никаких изменений данных.

---

## Совместимость

* PostgreSQL 11+ (рекомендуется 12+).
* `psycopg` v3.

---

## Диагностика

* Ошибка аутентификации → проверьте DSN.
* `timeout` → увеличьте `--statement-timeout-ms`.
* Нет схем → уточните фильтры.

---

## Лицензия

MIT