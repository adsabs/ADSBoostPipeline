# -*- coding: utf-8 -*-

from builtins import str
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float

import sys
from adsputils import get_date, UTCDateTime

Base = declarative_base()


class BoostFactors(Base):
    __tablename__ = 'boost_factors'
    id = Column(Integer, primary_key=True)
    bibcode = Column(String(19), index=True)
    scix_id = Column(String(19), index=True)
    created = Column(UTCDateTime, default=get_date)
    modified = Column(UTCDateTime, default=get_date, onupdate=get_date)
    
    # Basic boost factors
    refereed_boost = Column(Float)
    doctype_boost = Column(Float)
    recency_boost = Column(Float)
    boost_factor = Column(Float)
        
    # Collection weights
    astronomy_weight = Column(Float)
    physics_weight = Column(Float)
    earth_science_weight = Column(Float)
    planetary_science_weight = Column(Float)
    heliophysics_weight = Column(Float)
    general_weight = Column(Float)
    
    # Discipline-specific final boosts
    astronomy_final_boost = Column(Float)
    physics_final_boost = Column(Float)
    earth_science_final_boost = Column(Float)
    planetary_science_final_boost = Column(Float)
    heliophysics_final_boost = Column(Float)
    general_final_boost = Column(Float)
    