from __future__ import annotations

from sqlalchemy import BigInteger, Column, ForeignKey, String, Table
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class TypeOfBusiness(Base):
    __tablename__ = 'type_of_buisness'

    _id = Column(BigInteger, autoincrement=True, primary_key=True)
    code = Column(String(255), nullable=False, unique=True)
    parent_code = Column(String(255), nullable=False)
    section = Column(String(255), nullable=False)
    name = Column(String(1024), nullable=False)
    comment = Column(String)
    companies = relationship('CompanyDirectory')


class CompanyDirectory(Base):
    __tablename__ = 'company_directory'
    _id = Column(BigInteger, autoincrement=True, primary_key=True)
    name = Column(String(1024), nullable=False)
    full_name = Column(String(1024), nullable=False)
    inn = Column(String(255), nullable=False)
    kpp = Column(String(255), nullable=False)
    okved = Column(String(255), ForeignKey('type_of_buisness.code'))


vacancy_skills = Table(
    'vacancy_skills',
    Base.metadata,
    Column('_id', BigInteger, autoincrement=True, primary_key=True),
    Column('vacancy_id', ForeignKey('vacancy._id'), primary_key=True),
    Column('skill_id', ForeignKey('skill._id'), primary_key=True),
)

class Vacancy(Base):
    __tablename__ = 'vacancy'

    _id = Column(BigInteger, autoincrement=True, primary_key=True)
    company_name = Column(String(255), ForeignKey('company.name'))
    position = Column(String(1024), nullable=False)
    job_description = Column(String)
    key_skills = relationship(
        'Skill',
        secondary=vacancy_skills,
        back_populates='vacancies',
        )


class Skill(Base):
    __tablename__ = 'skill'

    _id = Column(BigInteger, autoincrement=True, primary_key=True)
    name = Column(String(1024), nullable=False)
    vacancies = relationship(
        'Vacancy',
        secondary=vacancy_skills,
        back_populates='key_skills',
        )


class Company(Base):
    __tablename__ = 'company'

    _id = Column(BigInteger, autoincrement=True, primary_key=True)
    name = Column(String(1024), nullable=False, unique=True)
    vacancies = relationship('Vacancy')
