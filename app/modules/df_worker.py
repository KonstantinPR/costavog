from app import app
from functools import reduce
import math
import requests
from app.models import Product, db
import datetime
import pandas as pd
import numpy as np
from app.modules import yandex_disk_handler
from app.modules import io_output


