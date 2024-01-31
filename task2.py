'''
Expectations on Second Task regarding matrix: 
1. User should be able to process any dataset which he wants. Give either table creation option or import csv, excel or 
   text file option
2. Calculated metrics should get stored in the database in some other created relation 'Metrics Output' - which can 
   consist columns as 'sr. no., table name, row count, column count.... etc.' 
3. So that when next time after rows are getting inserted in the customer table when any one of them are executing any of 
   the matrix it should update that particular cell in the 'Metrics Output' table.
4. In short we want to make our App customise and user friendly.  Focus less on UI facility rather focus on smooth backend
   operation connecting Python with Database and handling relations in our RDBMS.
5. Last point, one more things we would like to add i.e. showing data types of each column to users. So when user click 
   on display button it should display column name with their datatypes. 
'''
import pyautogui
import streamlit as st
import pandas as pd
from typing import Optional
import sqlalchemy
from sqlalchemy import create_engine, text, inspect, MetaData, select, Table, String, insert
from sqlalchemy.schema import DropTable
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session
import matplotlib.pyplot as plt

engine = create_engine(
    "mysql+mysqlconnector://root:@localhost/bacancydemo", echo=True)
conn = engine.connect()


class Base(DeclarativeBase):
    pass


class MetricsD(Base):
    '''
     User Model Class
    '''
    __tablename__ = "metrics_output"
    id: Mapped[int] = mapped_column(
        primary_key=True, nullable=False, autoincrement=True)
    tablename: Mapped[str] = mapped_column(String(30))
    rowcount: Mapped[Optional[float]]
    columncount: Mapped[Optional[float]]
    duplicaterows: Mapped[Optional[float]]
    nullvalue: Mapped[Optional[float]]
    standardvalue: Mapped[Optional[float]]

    def __repr__(self) -> str:
        return f"MetricsD(id={self.id!r}, tablename={self.tablename!r}, rowcount={self.rowcount!r}, columncount={self.columncount!r}, duplicaterows={self.duplicaterows!r}, nullvalue={self.nullvalue!r}, standardvalue={self.standardvalue!r})"


class Main():
    '''Main Class'''
    _filen = ""
    columns = {}
    values = {}

    def __init__(self):
        pass

    def tables_name(self):
        inspector = inspect(engine)
        tablename = inspector.get_table_names()
        tablename.remove("metrics_output")
        return tablename

    def get_table_data(self, tablename):
        metadata = MetaData()
        table = Table(tablename, metadata, autoload_with=engine)
        stmt = select(table)
        result = conn.execute(stmt)
        data = pd.DataFrame(result)
        return data

    def create_columns(self, name, cols):
        with st.form(key='hello', clear_on_submit=True):
            name_container, datatpe_container = st.columns(2)
            fields = [name_container.text_input(
                f"Enter the {i+1} field name ") for i in range((cols))]
            fieldsDatatpe = [datatpe_container.selectbox(
                'Select Datatype', ('int', 'varchar', 'datetime', 'float', 'boolean'), key=i) for i in range((cols))]
            f_submit = st.form_submit_button('submit')
            if f_submit:
                print(fields)
                print(fieldsDatatpe)
                return fields, fieldsDatatpe

    def table_insert_data(self, name, data, columns):
        dataframe = pd.DataFrame(columns=data)
        # dataframe = dataframe[dataframe.columns[0:]]
        # columns.pop('#')

        for key in columns.keys():
            if columns[key] == "int":
                columns[key] = sqlalchemy.types.INTEGER()
            elif columns[key] == "varchar":
                columns[key] = sqlalchemy.types.NVARCHAR(length=255)
            elif columns[key] == "float":
                columns[key] = sqlalchemy.types.Float(
                    precision=3, asdecimal=True)
            elif columns[key] == "boolean":
                columns[key] = sqlalchemy.types.Boolean()
            elif columns[key] == "datetime":
                columns[key] = sqlalchemy.DateTime()

        # sql insert
        try:
            fetch = dataframe.to_sql(
                name, conn, if_exists='replace', dtype=columns)
        except Exception as vx:
            st.write(str(vx))

        return "Inserted Successfully"

    def read_file(self, file):
        '''read the fle uploaded'''
        filename = file.name.split(".")
        self._filen = filename[0]
        data = pd.read_csv(file)
        dataframe = pd.DataFrame(data)

        return dataframe

    def column_types(self, dataframe1):
        '''Get column types'''
        column_data = dataframe1.columns
        for column in column_data:
            st.write(f"{column}")
            option = st.selectbox(
                'Select Datatype', ('int', 'varchar', 'datetime', 'float', 'boolean'), key=column)

            self.columns[column] = option
        return self.columns

    def insert_data(self, dataframe, columns):
        # dataframe = dataframe[dataframe.columns[1:]]
        # columns.pop('#')
        for key in columns.keys():
            if columns[key] == "int":
                columns[key] = sqlalchemy.types.INTEGER()
            elif columns[key] == "varchar":
                columns[key] = sqlalchemy.types.NVARCHAR(length=255)
            elif columns[key] == "float":
                columns[key] = sqlalchemy.types.Float(
                    precision=3, asdecimal=True)
            elif columns[key] == "boolean":
                columns[key] = sqlalchemy.types.Boolean()
            elif columns[key] == "datetime":
                columns[key] = sqlalchemy.DateTime()

        # sql insert
        try:
            fetch = dataframe.to_sql(
                self._filen, conn, if_exists='replace', dtype=columns)
            if fetch:
                st.write("Inserted Successfully")
        except Exception as vx:
            st.write(str(vx))

    def metrics_details(self, dataframe):
        '''metrics calculation'''
        standard_deviation = {}
        metrics_data = {}
        tab1, tab2, tab3, tab4 = st.tabs(
            ["Counts", "Duplicate and Null Values", "Standard Deviation", "Histograms"])

        with tab1:
            st.write("Row and Column counts")
            # Row Count
            row_count = len(dataframe.axes[0])
            st.write(f'Total rows in {self._filen} is : {row_count}')

            # Column Count
            column_count = len(dataframe.axes[1])
            st.write(f'Total columns in {self._filen} is : {column_count}')

        with tab2:
            st.write("Duplicate and Null Values")
            # Duplicated Rows
            d = data.duplicated()
            duplicated_data = d.sum()
            st.write(f'Total duplicate rows in {
                     self._filen} is : {duplicated_data}')

            # Null Values
            null_values = dataframe.isnull().sum().sum()
            null_col = dataframe.isnull().sum()
            st.write(f'Total Null values in {self._filen} is : {null_values}')
            st.write(null_col)

        with tab3:
            # Standard Deviation
            st.write("Standard Deviation")
            column_data = dataframe.select_dtypes(include='int64').columns

            if len(column_data) > 1:
                for column in column_data:
                    standard_deviation[column] = dataframe[column].std()
                st.write(standard_deviation)
            else:
                st.write("File has no columns of type integer")

        with tab4:
            st.write("Histogram")
            column_data = dataframe.select_dtypes(include='int64').columns
            plt.rcParams["figure.figsize"] = (5, 2.5)
            if len(column_data) > 1:
                for column in column_data:
                    fig, ax = plt.subplots()
                    st.write(column)
                    ax.hist(dataframe[column])
                    st.pyplot(fig)
            else:
                st.write("File has no columns of type integer")

        metrics_data["filename"] = self._filen
        metrics_data["row_count"] = row_count
        metrics_data["column_count"] = column_count
        metrics_data["duplicated_data"] = duplicated_data
        metrics_data["null_values"] = null_values
        metrics_data["null_col"] = null_col
        metrics_data["standard_deviation"] = standard_deviation

        return metrics_data

    def metrics_upload(self, tablename, datadict):
        rowcount = float(datadict['row_count'])
        columncount = float(datadict['column_count'])
        duplicaterows = float(datadict['duplicated_data'])
        nullvalue = float(datadict['null_values'])
        standardvalue = 0

        session = Session(engine)
        data = MetricsD(tablename=tablename, rowcount=rowcount,
                        columncount=columncount, duplicaterows=duplicaterows, nullvalue=nullvalue, standardvalue=standardvalue)
        session.add(data)
        session.commit()
        return "Inserted Successfully"

    def crud_get_col(self, data, dtype):
        dict = {}
        for item in data:
            if "INTEGER" in str(dtype[item]):
                value = st.number_input(f'Enter {item}', key=item)
            elif "BIGINT" in str(dtype[item]):
                value = st.number_input(f'Enter {item}', key=item)
            else:
                value = st.text_input(f'Enter {item}', key=item)
            dict[item] = value

            # self.values[item] = option
        return dict

    def crud_insert_table(self, values, tablename):
        colname = []
        value = []
        for i in values.keys():
            colname.append(i)
            value.append(values[i])
        st.write(value)

        # stmt = (
        #     tablename.insert().values([{'item' : item} for item in colname])
        # )
        columns = ', '.join("`" + str(x).replace('/', '_') +
                            "`" for x in values.keys())
        value = ', '.join("'" + str(x).replace('/', '_') +
                          "'" for x in values.values())
        sql = "INSERT INTO %s ( %s ) VALUES ( %s );" % (
            tablename, columns, value)
        conn.execute(text(sql))
        conn.commit()

        # result = conn.execute(insert(tablename), [values])
        # conn.commit()


task = Main()

tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["Create Table", "Upload CSV", "Metrics calculation", "CRUD", "Drop table"])

with tab1:
    st.header("Create Table")
    name = st.text_input("Enter table name")
    cols = st.number_input("Enter number of columns")
    cl = int(cols)
    if cl:
        data = task.create_columns(name, cl)
        # st.write(data[1])
        if data:
            res = {}
            for key in data[0]:
                for value in data[1]:
                    res[key] = value
                    data[1].remove(value)
                    break
            # st.write(res)
            result = task.table_insert_data(name, data[0], res)
            st.write(result)

with tab2:
    st.header("Upload CSV file")
    uploaded_file = st.file_uploader("Upload Csv File", type='csv',
                                     accept_multiple_files=False, disabled=False, label_visibility="visible")

    if uploaded_file:
        data = task.read_file(uploaded_file)
        st.write(data)

        with st.expander("Insert Data"):
            columns = task.column_types(data)
            if st.button("Insert Details"):
                task.insert_data(data, columns)


with tab3:
    tabname = task.tables_name()
    name = option = st.selectbox(
        'Select Table', tabname, key="dynamic crud")

    if st.button("Submit"):
        data = task.get_table_data(name)
        with st.expander("Metrics Data"):
            metrics_data = task.metrics_details(data)
            if metrics_data:
                dat = task.metrics_upload(name, metrics_data)

    # if st.button("Insert Metrics"):
    #     dat = task.metrics_upload(name,metrics_data)


with tab4:
    tabname = task.tables_name()
    tb = ["none"]
    for i in tabname:
        tb.append(i)
    name = st.selectbox(
        'Select Table', tb, key="metrics", placeholder="none")

    if name != "none":
        column_name = []
        tabletypes = {}
        t = Table(name, MetaData())
        inspector1 = inspect(engine)
        tablename = inspector1.get_columns(name)
        for i in tablename:
            tabletypes[i["name"]] = i['type']
        for i in tablename:
            column_name.append(i["name"])
        with st.expander("Insert Data"):
            result = task.crud_get_col(column_name, tabletypes)
            if st.button("Insert"):
                st.write(task.crud_insert_table(result, t))


with tab5:
    tabname = task.tables_name()
    name = option = st.selectbox(
        'Select Table', tabname, key="drop")
    if st.button("Drop"):
        t = Table(name, MetaData())
        if conn.execute(DropTable(t)):
            st.write("Table dropped")
            pyautogui.hotkey('f5')
