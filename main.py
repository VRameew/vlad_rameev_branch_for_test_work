from sql_connector import SQL_conn, Data, Documents
from data_filler import make_data, make_documents
from sqlalchemy import null
import json

base = SQL_conn()

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
        print("Нет документов для обработки")
        raise SystemExit(1)

    return data_document


def decod_document_data(data_document):
    try:
        decod_data = json.loads(data_document.document_data)
        return decod_data
    except Exception as e:
        print("Ошибка чтения данных документа: ", e)
        raise SystemExit(1)


def objects_data(decod_data):
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
        raise SystemExit(1)


def load_objects():
    data_for_objects = decod_document_data(doc_data())
    objects = objects_data(data_for_objects)
    session = base.get_session()

    if not objects:
        print("Нет данных объектов для обработки")
        raise SystemExit(1)

    try:
        print("START OBJ!!!!!!!!!!!!!")
        for obj in objects:
            print("=====OBJ!!!!!!!!!!!!!")
            object_data = Data(**obj)
            operation_details = obj.get("operation_details")
            if operation_details:
                for field, values in operation_details.items():
                    old_value = values.get("old")
                    new_value = values.get("new")
                    if old_value and getattr(object_data, field) == old_value:
                        setattr(object_data, field, new_value)

            session.merge(object_data)

        session.commit()
        session.close()

    except Exception as e:
        print("Ошибка при обработке данных объектов:", e)
        raise SystemExit(1)

load_objects()
