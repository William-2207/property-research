from __future__ import annotations
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from .models import Base


_engine = None
_SessionLocal = None


def init_engine(db_path: str):
    global _engine, _SessionLocal
    _engine = create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False},
        echo=False,
    )
    Base.metadata.create_all(_engine)
    _SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False)
    return _engine


def get_engine():
    if _engine is None:
        raise RuntimeError("Database not initialised. Call init_engine() first.")
    return _engine


@contextmanager
def get_session() -> Session:
    if _SessionLocal is None:
        raise RuntimeError("Database not initialised. Call init_engine() first.")
    session: Session = _SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def upsert_suburb(session: Session, name: str, state: str, city: str, **kwargs):
    from .models import Suburb
    suburb = session.query(Suburb).filter_by(name=name, state=state).first()
    if suburb is None:
        suburb = Suburb(name=name, state=state, city=city, **kwargs)
        session.add(suburb)
        session.flush()
    else:
        for k, v in kwargs.items():
            if v is not None:
                setattr(suburb, k, v)
    return suburb
