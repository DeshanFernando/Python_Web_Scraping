import logging
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import wincred
import pyodbc
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

class BusinessException(Exception):
    '''
    This function is isused to create a Exception object for issues related to business scenarios.
    eg:- Unable to identify login URL in Config file.
    '''
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

project_folder = r'C:\Users\DeshanFernando\PycharmProjects\Automations'

config_file_path = project_folder + r'\venv\Data\Config.xlsx'

def read_config(path=config_file_path):
    '''
    This function is used to read the entire Config file

    :param path (str): Path to the Configuartion file where process related configurations and settings are stored
    :return (dataframe): Read configuration data
    '''
    config = pd.read_excel(config_file_path, sheet_name='Settings')
    return config

# Setting a global variable for config data
config = read_config()

def read_config_value(key, config=config):
    '''
    Reads a value from global Config variable

    :param key (str): Config data name in Name column in Config file
    :param config (dataframe): Global Config data variable
    :return (str): Read value from Config variable
    '''
    if len(config[config['Name'] == key]['Value'].values) == 1 and \
            config[config['Name'] == key]['Value'].values[0] != '' and \
            (type(config[config['Name'] == key]['Value'].values[0]) == str or type(config[config['Name'] == key]['Value'].values[0]) == int):
        return config[config['Name'] == key]['Value'].values[0]
    else:
        return None

def login():
    '''
    This function is used to login to ACME website. It gets login URL from Config and gets login
    credentials from Windows Credential Manager

    :return (requests.Session): Logged in session
    '''
    url = read_config_value(key='login_url', config=config)
    cred_name = read_config_value(key='ACME_credential_name', config=config)
    if url is None:
        raise BusinessException("Unable to identify login URL in Config file.")
    if cred_name is None:
        raise BusinessException("Unable to identify ACME credentail name in Config file.")

    username, password = wincred.get_generic_credential(cred_name)
    if username is None:
        raise BusinessException("Login credentials cannot be found in Windows Credential Manager.")

    data = {
            '_token': '-empty-',
            'email': username,
            'password': password
        }

    with requests.Session() as s:
        # First GET page
        response = s.get(url=url)

        # Search fresh token in HTML
        soup = bs(response.text, 'html.parser')
        token = soup.find('input', {'name': "_token"})['value']
        # print('token:', token)

        # Run POST with new token
        data['_token'] = token
        response = s.post(url=url, data=data)
        if response.status_code != 200:
            raise Exception("Unable to login into ACME Website.")
        else:
            return s

# Function to scrape table data from ACME website
def scraping(session):
    '''
    Responsible of scraping work items table data from ACME website.

    :param session (requests.Session): Logged in session created by Login function
    :return (dataframe): Scraped and filtered work items
    '''
    url = read_config_value(key='extract_url')
    condition = True
    row_data = []

    while condition:
        if url is not None:
            # Get page data
            page = session.get(url)
            if page.status_code != 200:
                raise BusinessException("Unable to extract data from provided URL.")
        else:
            raise BusinessException('Unable to identify the URL for extraction.')

        soup = bs(page.text, 'html.parser')
        table_body = soup.find('table')

        # Iterating through html table and extract row by row
        for row in table_body.find_all('tr'):
            col = row.find_all('td')
            col = [ele.text.strip() for ele in col]
            row_data.append(col)

        # Next page
        pg = soup.find('ul', 'page-numbers')
        active_pg = pg.find('li', attrs={'aria-current':'page'})
        try:
            next_url = active_pg.findNextSibling('li').find('a').get('href')
        except:
            next_url = ''

        # Check whether next link is blank
        if next_url != '':
            url = next_url
        else:
            condition = False

    # Extracting table headers
    headers = []
    for i in soup.find_all('th'):
        col_name = i.text.strip()
        headers.append(col_name)

    # Get final table and filter to get only Open work items
    results = pd.DataFrame(row_data, columns=headers)
    results = results[results['Status'] == 'Open']
    # Remove unnecessary columns
    results = results.loc[:, results.columns != 'Actions']
    return results

def insert_into_db(results):
    '''
    This function is used to insert extracted ACME work items into SQL Server database.

    :param results (dataframe): Scraped and filtered work items
    '''
    if results.shape[0] > 0:
        # Getting rrquired parameters for SQL Server connection
        cred_name = read_config_value(key='DB_credential_name', config=config)
        server = read_config_value(key='DB_server_name', config=config)
        username, password = wincred.get_generic_credential(cred_name)
        database = 'Automations'

        if server is None or username is None:
            raise BusinessException('Unable to identify required parameters for SQL Server connection.')

        cnxn = pyodbc.connect(
            'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
        cursor = cnxn.cursor()
        # Insert results into SQL Server:
        for index, row in results.iterrows():
            cursor.execute("INSERT INTO [acme_work_items] (WIID, Description, Type, Status, Date) values(?,?,?,?,?)",
                           row.WIID, row.Description, row.Type, row.Status, row.Date)
        cnxn.commit()
        cursor.close()
    else:
        raise BusinessException('There are no work items available in ACME website to extract')

def send_email(user_type, subject, content, attachment_name=None, file=None):
    '''
    This function is used to send an email to either business or technical users. This can be configured in
    configuration file to send through Gmail or Outlook SMTP servers.

    :param user_type (str): This value can be business or technical
    :param subject (str): Email subject
    :param content (str): Email body content
    :param attachment_name (str): If attachment need to be sent, it's name
    :param file (str): If attachment need to be sent, it's file path
    '''
    # Parameters for sending email
    SERVER = read_config_value(key='email_server_name', config=config)
    PORT = read_config_value(key='email_port', config=config)
    cred_name = read_config_value(key='email_credential_name', config=config)
    username, password = wincred.get_generic_credential(cred_name)
    if user_type == 'business':
        TO = read_config_value(key='business_users', config=config)
    else:
        TO = read_config_value(key='technical_users', config=config)

    if SERVER is None or PORT is None or username is None or TO is None:
        raise BusinessException('Unable to identify required parameters for email SMPT settings.')

    # Creating message body object
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = username
    msg['To'] = TO
    # Handling attachments
    if file is not None:
        attachment = MIMEApplication(file.to_csv())
        attachment["Content-Disposition"] = 'attachment; filename="{}"'.format(attachment_name)
        msg.attach(attachment)
    msg.attach(MIMEText(content, 'html'))

    server = smtplib.SMTP(SERVER, PORT)
    server.set_debuglevel(0)
    server.ehlo()
    server.starttls()
    server.login(username, password)
    server.sendmail(username, TO, msg.as_string())
    server.quit()


def main():
    try:
        # Sending process initiation email
        send_email('business', 'ACME Work items Extraction Process - Initialization', '<p>Dear All,</p>'
                                                                                      '<p>Please be noted that <b>ACME Work items Extraction Process</b>'
                                                                                      ' has been initialized.</p>'
                                                                                      '<p>Thanks and Regards,<br>BOT</p>')
        # Login to ACME website
        session = login()
        # Scraping work items table data
        results = scraping(session)
        # Inserting extracted data into database
        insert_into_db(results)
        # Sending extracted data to business users
        send_email('business', 'ACME Work items Extraction Process - Completion', '<p>Dear All,</p>'
                                                                                  '<p><b>ACME Work items Extraction Process</b>'
                                                                                  ' has been completed. Please find attached file with extracted work items.</p>'
                                                                                  '<p>Thanks and Regards,<br>BOT</p>',
                   'work_items.csv', results)
    except Exception as ex:
        if 'BusinessException' in str(type(ex)):
            # Notifying business related errors to business users
            send_email('business', 'ACME Work items Extraction Process - Business Exception', '<p>Dear All,</p>'
                                                                                          '<p>Please be noted that bot execution failed '
                                                                                          'due to below error.</p><p>Error Message: '+ str(ex) + '</p><p>Thanks and Regards,<br>BOT</p>')
        else:
            # Notifying technical errors to technical users
            send_email('technical', 'ACME Work items Extraction Process - Technical Exception', '<p>Dear All,</p>'
                                                                                              '<p>Please be noted that bot execution failed '
                                                                                              'due to below error.</p><p>Error Message: ' + str(ex) + '</p><p>Thanks and Regards,<br>BOT</p>')

if __name__ == "__main__":
    main()