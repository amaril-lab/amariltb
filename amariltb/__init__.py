# This is so that you can import ppack or import average from ppack
# in stead of from ppack.functions import average

from .decorators import singleton
from .functions import listChunker, report, weirdCase
from .amaril_data_filters import E_FilterType,FilterPWWordCount,FilterDiagnoses,FilterLanguage,FilterCategory,FilterNumberInRange,FilterStringEquals,FilterBool,FilterString,FilterStringIsContainedBy,FilterColumnDoesExist
from .amaril_data_normelizer import E_Diagnosis 
from .amaril_data import AmarilData

