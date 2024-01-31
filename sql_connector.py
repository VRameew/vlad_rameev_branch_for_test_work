from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, DateTime, JSON, text

Base = declarative_base()


class SQL_conn():
    def __init__(self):
        print("__INIT__!!!!!!!!!")
        self.engine = create_engine("postgresql+psycopg2://user:password@postgres:5432/db",
                               echo=True, pool_pre_ping=True)
        print(self.engine)
        self.SessionLocal = sessionmaker(
            binds={Base: self.engine}, autocommit=False,
            autoflush=False, expire_on_commit=False, )
        print(self.SessionLocal())

    def get_session(self):
        session = self.SessionLocal()
        return session

    def create_tables(self):
        print("Создание")
        sql_data = '''
        CREATE TABLE IF NOT EXISTS public.data
        (
            object character varying(50) COLLATE pg_catalog."default" NOT NULL,
            status integer,
            level integer,
            parent character varying COLLATE pg_catalog."default",
            owner character varying(14) COLLATE pg_catalog."default",
            CONSTRAINT data_pkey PRIMARY KEY (object)
        )
        '''

        sql_documents = '''
        CREATE TABLE IF NOT EXISTS public.documents
        (
            doc_id character varying COLLATE pg_catalog."default" NOT NULL,
            recived_at timestamp without time zone,
            document_type character varying COLLATE pg_catalog."default",
            document_data jsonb,
            processed_at timestamp without time zone,
            CONSTRAINT documents_pkey PRIMARY KEY (doc_id)
        )
        '''
        print("создание Начало")
        with self.engine.connect() as eng:
            eng.execute(text(sql_data))
            eng.execute(text(sql_documents))
            print("создание завершено")
            eng.commit()
            eng.close()
            return True

        return False


class Data(Base):
    __tablename__ = 'data'

    object = Column(String(50), primary_key=True)
    status = Column(Integer)
    level = Column(Integer)
    parent = Column(String)
    owner = Column(String(14))

    def to_dict(self):
        return {key: value for key, value in self.__dict__.items() if key != '_sa_instance_state'}


class Documents(Base):
    __tablename__ = 'documents'

    doc_id = Column(String, primary_key=True)
    recived_at = Column(DateTime)
    document_type = Column(String)
    document_data = Column(JSON)
    processed_at = Column(DateTime)
