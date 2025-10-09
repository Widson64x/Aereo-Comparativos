# connection.py

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os
import logging
from urllib.parse import quote_plus
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import text 

logger = logging.getLogger(__name__)
load_dotenv()

db_user = os.getenv("DB_USER")
db_pass = quote_plus(os.getenv("DB_PASS", ""))
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

if not all([db_user, db_host, db_port, db_name]):
    raise Exception("Uma ou mais variáveis de banco de dados não estão definidas no .env!")

logger.info(f"Conectando ao banco de dados {db_name} no host {db_host}:{db_port} com o usuário {db_user}.")
DATABASE_URL = f"mssql+pymssql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

engine = create_engine(
    DATABASE_URL,
    poolclass=NullPool
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

logger.info("Engine SQLAlchemy configurada ✅")

def get_db():
    db = SessionLocal()
    try:
        yield db
    except SQLAlchemyError as e:
        logger.error(f"Erro na sessão do banco de dados: {e}")
        db.rollback()
        raise
    finally:
        db.close()
        
def test_connection():
    try:
        with engine.connect() as connection:
            # --- CORREÇÃO AQUI: Envolver a string SQL em text() ---
            result = connection.execute(text("SELECT TOP 10 * FROM tb_ctc_esp")) 
            logger.info("Conexão com o banco de dados bem-sucedida.")
            print( "10 primeiras linhas da tabela tb_ctc_esp:" )
            print( result.fetchall() )
            return True
    except SQLAlchemyError as e:
        # Se você ainda tiver problemas de conexão, o erro real aparecerá aqui.
        logger.error(f"Falha na conexão com o banco de dados: {e}")
        return False
    
if __name__ == "__main__":
    test_connection()