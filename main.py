from sql_connector import SQL_conn, Data, Documents
from data_filler import make_data, make_documents
from sqlalchemy import null
import json
from datetime import datetime

base = SQL_conn()
work_flag: bool = True #Для корректной работы цикла

if base.create_tables():
    """Создание сессии для формирования данных"""
    session = base.get_session()
    """Очистка данных в базе"""
    session.query(Data).delete()
    session.query(Documents).delete()
    session.commit()
    """Формированние данных"""
    data = make_data()

    """Добавление объектов Data в сессию"""
    for data_obj in data.values():
        data_row = Data(**data_obj)
        session.add(data_row)

    """Добавление объектов Documents в сессию"""
    documents = make_documents(data)
    for doc_data in documents:
        doc_row = Documents(**doc_data)
        session.add(doc_row)

    session.commit()
    session.close()
else:
    print("Создание таблиц и инициализация данных не произведены!")


def doc_data():
    "Получение данных по первому не обработтаному документу"
    session = base.get_session()
    data_document = session.query(Documents).filter(
        Documents.document_type == 'transfer_document',
        Documents.processed_at == null(),
    ).first()
    session.close()
    if data_document is None:
        global work_flag
        work_flag = False
        print("Нет документов для обработки")
        return False

    return data_document


def decod_document_data(data_document):
    """Получение данных из поля document_data и их преобразование из JSON"""
    try:
        decod_data = json.loads(data_document.document_data)
        return decod_data
    except Exception as e:
        print("Ошибка чтения данных документа: ", e)
        return False


def objects_data(decod_data):
    """Получение всех данных объектов по расшифрованным данным decod_document_data"""
    objects_data_full = []
    try:
        objects_data = decod_data.get('objects')
        if len(objects_data) == 0:
            return "Нет данных объектов для обработки"
        else:
            session = base.get_session()
            for record in objects_data:
                record_data = session.query(Data).filter(
                    Data.object == record).all()
                objects_data_full.append(record_data[0].to_dict())
            session.close()
        return objects_data_full
    except Exception as e:
        print("Ошибка при чтении данных объектов:", e)
        return False


def load_objects():
    """Обработка данных по обьектам что были найдены
    сверка значений и их обработка"""
    document = doc_data() #получение документа
    data_for_objects = decod_document_data(document) #получение данных из документа
    objects = objects_data(data_for_objects) # разбиение на обьекты для обработки
    session = base.get_session()

    if not objects:
        print("Нет данных объектов для обработки")
        return False

    try:
        for obj in objects:
            object_data = Data(**obj)  # создание обектов типа Data для работы в SQLalchemy
            operation_details = obj.get("operation_details")
            if operation_details:
                for field, values in operation_details.items(): # Обработка поля operation_details
                    old_value = values.get("old")
                    new_value = values.get("new")
                    if old_value and getattr(object_data, field) == old_value:
                        setattr(object_data, field, new_value)
            session.merge(object_data)

        document.processed_at = datetime.now() # Указание что документ обработан
        session.merge(document)
        session.commit()
        session.close()
        return True

    except Exception as e:
        print("Ошибка при обработке данных объектов:", e)
        return False


while work_flag:
    load_objects()
print("Обработка обьектов завершена! ")

