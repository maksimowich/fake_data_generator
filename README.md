*fake_data_generator* – библиотека для генерации искусственных данных (работает с Hive и Impala).

Библиотека предоставляет три функции:

- *generate_fake_table* – функция генерации искусственных данных в таблицу  
Обязательные параметры:
  - **conn** – подключение к базе данных (спарк сессия или движок sqlalchemy)
  - **source_table_name_with_schema** – название исходной таблицы со схемой (по этой таблице вычисляются паттерны для генерации данных)
  - **dest_table_name_with_schema** – название таблицы, в которую будут добавлены сгенерированные данные (если указанная таблица не существуют, то она будет создана)
  - **number_of_rows_to_insert** – количество строк, которое будет сгенерировано и вставлено в таблицу
  - **number_of_rows_from_which_to_create_pattern** – количество строк, из которых будут выявлены паттерны для генерации данных (из исходной таблицы дел

  Необязательные параметры:
  - **columns_info** – дополнительная информация о генерации данных для колонок таблицы (данный параметр принимает список объектов Column)
  - **columns_to_include** – названия колонок, которые должны быть включены в создаваемую таблицу
  - **batch_size** – количество строк, которые будут сгенерированы и вставлены в таблицы в одной итерации (генерация и вставка строк в таблицу происходит итерационно)


- *generate_table_profile* – функция, создающая файл-профиль таблицы в формате json. По файлу-профилю можно сгенерировать данные в таблицу с помощью функции *generate_table_from_profile*.  
Обязательные параметры:
  - **conn** – подключение к базе данных (спарк сессия или движок sqlalchemy)
  - **source_table_name_with_schema** – название исходной таблицы со схемой (по этой таблице вычисляются паттерны для генерации данных, которые будут сохранены в файл-профиль)
  - **output_table_profile_path** – название файла-профиля таблицы
  - **number_of_rows_from_which_to_create_pattern** – количество строк, из которых будут выявлены паттерны для генерации данных (из исходной таблицы дел

  Необязательные параметры:
  - **columns_info** – дополнительная информация о генерации данных для колонок таблицы (данный параметр принимает список объектов Column)
  - **columns_to_include** – названия колонок, которые должны быть включены в файл-профиль

  
- *generate_table_from_profile* – функция генерации искусственных данных в таблицу по файлу-профилю.  
Обязательные параметры:
  - **conn** – подключение к базе данных (спарк сессия или движок sqlalchemy)
  - **source_table_profile_path** – название файла-профиля таблицы
  - **dest_table_name_with_schema** – название таблицы, в которую будут добавлены сгенерированные данные (если указанная таблица не существуют, то она будет создана)
  - **number_of_rows_to_insert** – количество строк, которое будет сгенерировано и вставлено в таблицу

  Необязательные параметры:
  - **columns_info** – дополнительная информация о генерации данных для колонок таблицы (данный параметр принимает список объектов Column)
  - **batch_size** – количество строк, которые будут сгенерированы и вставлены в таблицы в одной итерации (генерация и вставка строк в таблицу происходит итерационно)


В зависимости от типа колонки и дополнительной информации, переданной о ней, алгоритм генерации искусственных данных может различаться. Дополнительная информация по каждому типу колонки описывается соответствующим классом.
Вне зависимости от типа колонки указывается её название и тип данных (параметры column_name и data_type).  
Для описания колонок библиотека предоставляет классы CategoricalColumn, DecimalColumn, IntColumn, TimestampColumn, DateColumn, StringColumn.