from typing import List

from bs4 import BeautifulSoup as Soup
from pydantic import BaseModel, Field, validator

class CompanyDir(BaseModel):
    """
    """
    inn: str
    kpp: str
    okved: str
    name: str
    full_name: str

class Company(BaseModel):
    """
    """
    name: str = Field(alias='company_name')

class Skill(BaseModel):
    """
    """

    name: str
    
class Vacancy(BaseModel):
    """
    """

    position: str
    company_name: str
    job_description: str
    key_skills: List[Skill]


class SearchItem(BaseModel):
    """
    """

    id: str = Field(alias='id')


class ResponseModel(BaseModel):
    """
    """

    items: List[SearchItem]
    page: int
    pages: int


class VacancyModel(BaseModel):
    """
    """

    position: str = Field(alias='name')
    company_name: str
    job_description: str = Field(alias='description')
    key_skills: List[Skill]

    @validator('job_description')
    def delete_markup(cls, value):
        return Soup(value, 'lxml').getText()

    def __init__(self, **kwargs):
        kwargs['company_name'] = kwargs['employer']['name']
        super().__init__(**kwargs)
