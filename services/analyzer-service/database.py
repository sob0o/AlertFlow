from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime

DATABASE_URL = "postgresql://alertflow:alertflow@postgres:5432/alertflow"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class MetricSnapshot(Base):
    __tablename__ = "metric_snapshots"

    id = Column(Integer, primary_key=True, index=True)
    merchant_id = Column(String, index=True)
    total_transactions = Column(Integer)
    success_rate = Column(Float)
    p50 = Column(Integer)
    p95 = Column(Integer)
    p99 = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)

def init_db():
    Base.metadata.create_all(bind=engine)