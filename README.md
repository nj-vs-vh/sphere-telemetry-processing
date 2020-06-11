# Телеметрия в эксперименте СФЕРА-2

Скрипты и ноутбуки:
## sphere_log_parser.py
— скрипт (также подключаемый как модуль) для парсинга файлов логов в стандартном формате СФЕРА-2. Разделитель записей в логе настраивается, параметр по умолчанию — строка, содержащая `-----` (5 дефисов). Обязательное поле лога: дата/время. Опциональные поля: GPS (считываются широта, долгота и высота, время), барометры (считывается давление в мм. водяного столба, переводится в гектопаскали), термометры (в градусах цельсия). Остальные поля при необходимости легко добавить в `parse_log_record` (подфункция `parse_current_line` с соответствующими параметрами).

При использовании как модуля:
```python
import sphere_log_parser as slp
ground_tele = slp.read_log_to_dataframe('log.txt')
ground_tele.to_csv('ground_data.tsv', sep='\t')
```
