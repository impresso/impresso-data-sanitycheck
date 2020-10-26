FROM ic-registry.epfl.ch/dhlab/impresso_pycommons:v1

# Add local impresso_data_sanitycheck
ADD . .

RUN pip install --upgrade pip

RUN python setup.py install
