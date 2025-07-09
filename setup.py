from setuptools import find_packages,setup

HYPHEN_E_DOT = '-e .'

def get_req(file_path):
    '''
        file_path:- path to the file where library requirements are present
        returns list of libraries required
    '''

    reqs = []
    with open(file_path) as file_obj:
        reqs = file_obj.readlines()
        # Strip whitespace (including \n) from each line
        reqs = [rq.strip() for rq in reqs if rq.strip() != '']

        # it will read -e . from requirements and to remove it following condition will execute
        if HYPHEN_E_DOT in reqs:
            reqs.remove(HYPHEN_E_DOT)

    return reqs



setup(

    name='forecasting_project',
    version='0.0.1',
    author='Netra',
    author_email='kulkarninetra5@gmail.com',
    packages=find_packages(),
    install_requires=get_req('requirements.txt'),


)