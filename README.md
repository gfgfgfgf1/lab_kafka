# Лабораторная работа #1

## Project structure:
    .
    ├── data                                                # папка под данные, необходимые для/полученные при работе
    │   └── US_Accidents_March23_sampled_500k.csv           # версия датасета для Kafka
    ├── scripts                                             # папка под скрипты
    │   ├── generation.py                                   # скрипт, имитирующий генерацию данных
    │   ├── processing.py                                   # скрипт, обрабатывающий входящие данные
    │   ├── Models.py                                       # скрипт для обучения модели
    │   └── utils.py                                        # файл с Producer and Consumer
    ├── run.py                                              # файл для быстрого запуска проекта
    ├── lab_kafka.ipynb                                     # Ipynb файл с разбором датасета и графиками
    ├── docker-compose.yml                                  # описание Docker контейнеров
    ├── README.md                                           # README
    └── requirements.txt                                    # файл с библиотеками которые требуеются для корректной работы проекта

## Setup

1. Install requirements:
    
    python -m pip install -r requirements.txt

2. Run the script:
    
    python ./run.py

## Датасет:

https://drive.google.com/file/d/1U3u8QYzLjnEaSurtZfSAS_oh9AT2Mn8X