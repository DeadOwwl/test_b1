DROP TABLE random_data CASCADE;

-- Оптимальнее, конечно, было бы с id, но пока без него.
CREATE TABLE random_data(random_date DATE NOT NULL,
                        random_latin_string VARCHAR(12) NOT NULL,
                        random_cyrillic_string VARCHAR(12) NOT NULL,
                        random_integer_number INTEGER NOT NULL,
                        random_double_number NUMERIC NOT NULL);
