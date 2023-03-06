import streamlit as st
import datetime
from datetime import timedelta
import streamlit_authenticator as stauth
import yaml
from yaml import SafeLoader

from optiml.backend.cost_profile import get_previous_dates

params = dict(
    Schema="KIV.ACCOUNT_USAGE",
    sdate=datetime.datetime.strptime('2022-10-05', "%Y-%m-%d").date(),
    edate=datetime.datetime.strptime('2022-10-12', "%Y-%m-%d").date(),
    TRAINING_LENGTH=30,
)


def set_params():
    for k, v in params.items():
        print(k, v)
        st.session_state[k] = v
    st.session_state.p1_sdate, st.session_state.p1_edate = get_previous_dates(st.session_state.sdate,
                                                                              st.session_state.edate, 1)
    st.session_state['training_start'] = st.session_state["sdate"] - timedelta(st.session_state['TRAINING_LENGTH'] + 1)
    st.session_state['training_end'] = st.session_state["sdate"] - timedelta(1)


def authenticate_user(username, password):
    with open('user.txt') as fp:
        data = fp.read()
    saved_user = data.split()[0].lower()
    saved_password = data.split()[1]
    print("saved: ", saved_password, saved_password)
    if saved_user == username.lower() and saved_password == password:
        return True
    return False


def change_state(key, value):
    st.session_state[key] = value


names = ["Peter Parker", "Rebecca Miller"]
usernames = ["admin"]
passwords = ['admin']
with open('config.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)
#
# authenticator = stauth.Authenticate(
#     config['credentials'],
#     config['cookie']['name'],
#     config['cookie']['key'],
#     config['cookie']['expiry_days'],
#     config['preauthorized']
# )

authenticator = stauth.Authenticate(names, usernames, passwords,
                                    "dashboard", "abcdef", cookie_expiry_days=30)
