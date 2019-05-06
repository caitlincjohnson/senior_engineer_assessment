import pandas as pd
import logging
import pyodbc
import os
import queries as q


logging.basicConfig(level=logging.INFO)


def extract(copyintodate, filename, providergroup):
    """
    Extracts the data from the Excel spreadsheet and returns a DataFrame
    :type copyintodate: String
    :type filename: String
    :type providergroup: String
    :rtype: DataFrame
    """
    try:
        logging.info('Importing Excel file for %s on the date of %s' % (providergroup, copyintodate))
        data = pd.read_excel(r'%s.xlsx' % filename, skiprows=3, skipfooter=3)
        return data
    except Exception as e:
        logging.info('Not able to read data. Error: %s' % e)
        return


def transform_demographics(data):
    """
    Transforms the data for the Demographic table into the desired format
    :type data: DataFrame
    :rtype: DataFrame
    """
    logging.info('Transforming Demographic data')
    df_demographics = pd.DataFrame(data, columns=['ID', 'First Name', 'Middle Name', 'Last Name', 'DOB[1]', 'Sex',
                                                  'Favorite Color'], index=[0])
    logging.info('Replacing the Sex values with M for 0 and F for 1')
    df_demographics['Sex'] = df_demographics['Sex'].map({0: 'M', 1: 'F'})
    logging.info('Replacing existing middle names with the first initial only')
    df_demographics['Middle Name'] = df_demographics['Middle Name'].str[:1]
    logging.info('Converting the existing DOBs datetimes to ISO-8601 format')
    df_demographics['DOB[1]'] = df_demographics['DOB[1]'].apply(lambda x: x.isoformat())
    return df_demographics


def transform_riskquarter(data):
    """
    Transforms the data for the RiskByQuarter table into the desired format
    :type data: DataFrame
    :rtype: DataFrame
    """
    logging.info('Transforming RiskByQuarter data')
    df_quarters_risk_quarters = pd.DataFrame(data
                                             , columns=['ID', 'Risk Q1', 'Risk Q2 ', 'Risk Increased Flag']
                                             , index=[0])
    df_quarters_risk_scores = pd.DataFrame(data
                                           , columns=['ID', 'Attributed Q1', 'Attributed Q2', 'Risk Increased Flag']
                                           , index=[0])
    logging.info('Dropping all rows with a value of No for Risk Increased Flag')
    df_quarters_risk_quarters = df_quarters_risk_quarters[df_quarters_risk_quarters['Risk Increased Flag'] != ' No']
    df_quarters_risk_scores = df_quarters_risk_scores[df_quarters_risk_scores['Risk Increased Flag'] != ' No']
    logging.info('Dropping the Risk Increased Flag column')
    df_quarters_risk_quarters = df_quarters_risk_quarters.drop(columns='Risk Increased Flag')
    df_quarters_risk_scores = df_quarters_risk_scores.drop(columns='Risk Increased Flag')
    logging.info('Unpivoting data sets')
    df_quarters_risk_quarters = pd.melt(df_quarters_risk_quarters, id_vars=['ID'], var_name=['Quarter'],
                                        value_name='RiskScore')
    df_quarters_risk_scores = pd.melt(df_quarters_risk_scores, id_vars=['ID'], var_name=['Quarter'],
                                      value_name='AttributedFlag')
    logging.info('Setting values within the Quarter column to Q1 or Q2')
    df_quarters_risk_quarters['Quarter'] = df_quarters_risk_quarters['Quarter'].map({'Risk Q1': 'Q1', 'Risk Q2 ': 'Q2'})
    df_quarters_risk_scores['Quarter'] = df_quarters_risk_scores['Quarter'].map(
        {'Attributed Q1': 'Q1', 'Attributed Q2': 'Q2'})
    logging.info('Setting an index within the data sets based on ID and Quarter')
    df_quarters_risk_quarters = df_quarters_risk_quarters.set_index(
        ['ID', 'Quarter', df_quarters_risk_quarters.groupby(['ID', 'Quarter']).cumcount()])
    df_quarters_risk_scores = df_quarters_risk_scores.set_index(
        ['ID', 'Quarter', df_quarters_risk_scores.groupby(['ID', 'Quarter']).cumcount()])
    logging.info('Concatenating the data sets based on the index and simultaneously dropping the index column')
    df_quarters_risk = (pd.concat([df_quarters_risk_quarters, df_quarters_risk_scores], axis=1)
                        .sort_index(level=2)
                        .reset_index(level=2, drop=True)
                        .reset_index())
    return df_quarters_risk


def load(conn, copyintodate, cursor, df_demographics, df_quarters_risk, providergroup):
    """
    Loads the DataFrames for the Demographic and RiskByQuarter tables into the targeted PersonDatabase
    :type conn: Object
    :type copyintodate: String
    :type cursor: Cursor
    :type df_demographics: DataFrame
    :type df_quarters_risk: DataFrame
    :type providergroup: String
    """
    logging.info('Inserting the formatted data into Demographic table')
    for index, row in df_demographics.iterrows():
        cursor.execute(q.insert_demographics_query.format(
            row['ID']
            , row['First Name']
            , row['Middle Name']
            , row['Last Name']
            , row['DOB[1]']
            , row['Sex']
            , row['Favorite Color']
            , providergroup
            , copyintodate
        ))
        conn.commit()
    logging.info('Inserting the formatted data into RiskByQuarter table')
    for index, row in df_quarters_risk.iterrows():
        cursor.execute(q.insert_quarter_risk_query.format(
            row['ID']
            , row['Quarter']
            , row['AttributedFlag']
            , row['RiskScore']
            , copyintodate
        ))
        conn.commit()


def etl_script(filename):
    """
    The main script that calls the appropriate functions to complete the ETL process
    :type filename: String
    """
    providergroup, date_integer = filename.rsplit(' ', 1)
    copyintodate = '%s/%s/20%s' % (date_integer[:2],date_integer[2:4],date_integer[4:])
    data = extract(copyintodate, filename, providergroup)
    df_demographics = transform_demographics(data)
    df_quarters_risk = transform_riskquarter(data)
    connection_path = 'DRIVER=%s;PORT=%s;DSN=%s;UID=%s;PWD=%s;Database=%s' % (os.environ['DRIVER']
                                                                              , os.environ['PORT']
                                                                              , os.environ['DSN']
                                                                              , os.environ['USER']
                                                                              , os.environ['PWD']
                                                                              , 'PersonDatabase')
    logging.info('Connecting to SQL Server')
    conn = pyodbc.connect(connection_path)
    logging.info('Establishing cursor')
    cursor = conn.cursor()
    try:
        logging.info('Creating the Demographic table')
        cursor.execute(q.create_demographics_table_query)
        conn.commit()
        logging.info('Creating the RiskByQuarter table')
        cursor.execute(q.create_risk_table_query)
        conn.commit()
        load(conn, copyintodate, cursor, df_demographics, df_quarters_risk, providergroup)
    except Exception as e:
        logging.info('An error occurred, see below for details:')
        logging.info(e)
    finally:
        logging.info('Closing cursor')
        cursor.close()
        logging.info('Closing connection to SQL Server')
        conn.close()


if __name__ == '__main__':
    etl_script('Privia Family Medicine 113018')