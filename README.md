**fake_data_generator** – библиотека для генерации табличных искусственных данных (работает с Hive и Impala)

### Библиотека предоставляет три функции:

- *generate_fake_table* – функция генерации искусственных данных в таблицу  
Обязательные параметры:
  - **conn** – подключение к базе данных (спарк сессия или движок sqlalchemy)
  - **source_table_name_with_schema** – название исходной таблицы со схемой (по этой таблице вычисляются паттерны для генерации данных)
  - **dest_table_name_with_schema** – название таблицы, в которую будут добавлены сгенерированные данные (если указанная таблица не существуют, то она будет создана)
  - **number_of_rows_to_insert** – количество строк, которое будет сгенерировано и вставлено в таблицу
  - **number_of_rows_from_which_to_create_pattern** – количество строк, из которых будут выявлены паттерны для генерации данных (из исходной таблицы
делается селект-запрос на данное количество строк)

  Необязательные параметры:
  - **columns_info** – дополнительная информация о генерации данных для колонок таблицы (данный параметр принимает список объектов Column)
  - **columns_to_include** – названия колонок, которые должны быть включены в создаваемую таблицу
  - **batch_size** – количество строк, которые будут сгенерированы и вставлены в таблицы в одной итерации (генерация и вставка строк в таблицу происходит итерационно)


Пример вызова функции:
````
generate_fake_table(conn=spark,
                    source_table_name_with_schema='test.table_name',
                    dest_table_name_with_schema='test.gen_table_name',
                    number_of_rows_to_insert=30,
                    number_of_rows_from_which_to_create_pattern=100,
                    columns_info=[CategoricalColumn(column_name='col_a')],
                    batch_size=10)
````
Данный вызов создаст таблицу, если она не создана, с именем *test.gen_table_name* (dest_table_name_with_schema),
сделает селект-запрос всех колонок на *100* строк (number_of_rows_from_which_to_create_pattern) к таблице *test.table_name* (source_table_name_with_schema) и выявит паттерны для генерации данных,
осуществит вставку в таблицу *test.gen_table_name* *30* строк (number_of_rows_to_insert) батчами по *10* строк (batch_size).
Также в параметре columns_info дополнительно указано, что колонка col_a считается категориальной.

- *generate_table_profile* – функция, создающая файл-профиль таблицы в формате json. По файлу-профилю можно сгенерировать данные в таблицу с помощью функции *generate_table_from_profile*.  
Обязательные параметры:
  - **conn** – подключение к базе данных (спарк сессия или движок sqlalchemy)
  - **source_table_name_with_schema** – название исходной таблицы со схемой (по этой таблице вычисляются паттерны для генерации данных, которые будут сохранены в файл-профиль)
  - **output_table_profile_path** – название файла-профиля таблицы
  - **number_of_rows_from_which_to_create_pattern** – количество строк, из которых будут выявлены паттерны для генерации данных (из исходной таблицы дел

  Необязательные параметры:
  - **columns_info** – дополнительная информация о генерации данных для колонок таблицы (данный параметр принимает список объектов Column)
  - **columns_to_include** – названия колонок, которые должны быть включены в файл-профиль

Пример вызова функции:
````
generate_table_profile(conn=spark,
                       source_table_name_with_schema='test.table_name',
                       output_table_profile_path='test.table_name.json',
                       number_of_rows_from_which_to_create_pattern=1000,
                       columns_info=[
                           StringColumn(column_name='t_changed_dttm_str', string_copy_of='t_changed_dttm'),
                       ])
````
Данный вызов функции создаст файл-профиль таблицы в формате json, файл будет назван *test.table_name.json* (output_table_profile_path).
Паттерны для генерации данных будут выявлены из выборки *1000* строк (number_of_rows_from_which_to_create_pattern) из таблицы *test.table_name*.
Также в параметре columns_info передана дополнительная информация о том, как генерировать данные для конкретных колонок.

- *generate_table_from_profile* – функция генерации искусственных данных в таблицу по файлу-профилю.  
Обязательные параметры:
  - **conn** – подключение к базе данных (спарк сессия или движок sqlalchemy)
  - **source_table_profile_path** – название файла-профиля таблицы
  - **dest_table_name_with_schema** – название таблицы, в которую будут добавлены сгенерированные данные (если указанная таблица не существуют, то она будет создана)
  - **number_of_rows_to_insert** – количество строк, которое будет сгенерировано и вставлено в таблицу

  Необязательные параметры:
  - **columns_info** – дополнительная информация о генерации данных для колонок таблицы (данный параметр принимает список объектов Column)
  - **batch_size** – количество строк, которые будут сгенерированы и вставлены в таблицы в одной итерации (генерация и вставка строк в таблицу происходит итерационно)

Пример вызова функции:
````
generate_table_from_profile(conn=spark,
                            source_table_profile_path='test.table_name.json',
                            dest_table_name_with_schema='test.gen_table_name',
                            number_of_rows_to_insert=10,
                            batch_size=30)
````
Данный вызов создаст таблицу, если она не создана, с именем *test.gen_table_name* (dest_table_name_with_schema) и
осуществит генерацию данных (паттерны для генерации берутся из файла-профиля *test.table_name.json*) и
вставку в таблицу *test.gen_table_name* *30* строк (number_of_rows_to_insert) батчами по *10* строк (batch_size).

#### Алгоритмы генерации данных

Всего есть три алгоритма генерации данных:
1) для категориальной колонки производится случайная выборка из исходных или переданных в качестве параметра значений с учетом вероятности;
2) для некатегориальной нестроковой колонки производится случайная генерация значений из оцененной или переданной в качестве параметра плотности непрерывного распределения;
3) для некатегориальной строковой колонки производится случайная генерация значений по вычисленному или переданному в качестве параметра общему регулярному выражению.

#### Логика по умолчанию определения алгоритма генерации данных для колонки

Колонка считается категориальной, если
либо отношение количества уникальных значений к количеству всех строк меньше 0.2
либо количество уникальных значений равно 0 или 1. Колонка типа decimal категориальной быть не может.
Чтобы переопределить логику по умолчанию, необходимо передать информацию о генерации в параметре columns_info.

#### Библиотека предоставляет классы для описания различных типов колонок

- Категориальный:

  - *CategoricalColumn(column_name='col_a')* - генерация по колонке col_a будет происходить как для категориальной колонки;

  - *CategoricalColumn(column_name='col_a', values=[0, 1], probabilities=[0.3, 0.7])* - генерация по колонке col_a будет происходить как для категориальной с указанными вероятностями значений.

- Некатегориальный нестроковый:

  - *ContinuousColumn(column_name='col_a', intervals=[(10, 20), (20, 30)], probabilities=[0.3, 0.7])* - генерация из интервалов будет происходить с соответствующими указанными вероятностями, в интервале происходит выборка значения с равномерным распределением.

- Некатегориальный строковый: 

  - *StringColumn(column_name='col_s', common_regex='[0-9][0-9][a-b]')* - будет происходить генерация случайных строк, удовлетворящих указанному регулярному выражению;

  В параметре string_copy_of можно указать имя колонки исходной таблицы, чьей строковой копией будет указанная колонка:
  - *StringColumn(column_name='col_s', string_copy_of='col_b')* - колонка col_s будет строковой копией колонки col_b.

Для колонки типа timestamp можно генерировать значения текущей даты и времени:

  - *CurrentTimestampColumn(column_name='col_timestamp')*