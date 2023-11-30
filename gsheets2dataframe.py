import gspread
from pandas import DataFrame

from oauth2client.service_account import ServiceAccountCredentials


# Set the path to your credentials JSON file
CREDS_FILE = "<your-service-account-credential>.json"
# CREDS_DICT = {"your_json": "as a python dictionary"}


class GsToDfConverter:
    """
    A class for converting Google Sheets data to Pandas DataFrames.

    Parameters:
        - sheet_id (str): The Google Sheets document ID (from the URL)

    Attributes:
        - client: The gspread authorized client.
        - sheet_id (str): The provided Google Sheets document ID.
        - sheet: The Google Sheets document object.

    Raises:
        Exception: If there is an error initializing the G-Sheets to Dataframe Converter.

    Example:
        converter = GsToDfConverter("your_google_sheet_id")
        # Use the converter object for further operations.
    """

    def __init__(self, sheet_id):
        try:
            SCOPE = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
            ]
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                CREDS_FILE, SCOPE
            )  # or:
            # credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            #     CREDS_DICT, SCOPE
            # )

            client = gspread.authorize(credentials)

            self.client = client
            self.sheet_id = sheet_id
            self.sheet = client.open_by_key(sheet_id)
        except Exception as e:
            raise Exception(
                "Error initializing G-Sheets to Dataframe Converter: " + str(e)
            )

    def upload(self, df, worksheet_name):
        """
        Uploads a Pandas DataFrame to a specified worksheet in the Google Sheets document.

        Parameters:
            - df (pandas.DataFrame): The DataFrame to be uploaded.
            - worksheet_name (str): The name of the worksheet to upload the DataFrame to.

        Raises:
            gspread.exceptions.SpreadsheetNotFound: If the specified worksheet does not exist,
                a new worksheet is created before uploading the DataFrame.

        Note:
            If the worksheet is initially empty, the entire DataFrame is uploaded starting from cell A1.
            If the worksheet has existing data, the DataFrame rows are appended to the end.

        TODO:
            - Currently, the method appends rows without considering column order.
              Adjust this behavior if you want to match DataFrame columns to worksheet columns.

        Example:
            converter = GsToDfConverter("your_google_sheet_id")
            dataframe_to_upload = pd.DataFrame({'Column1': [1, 2, 3], 'Column2': ['A', 'B', 'C']})
            converter.upload(dataframe_to_upload, "Sheet1")
        """

        try:
            focus_tab = self.sheet.worksheet(worksheet_name)
        except gspread.exceptions.SpreadsheetNotFound:
            self.sheet.add_worksheet(worksheet_name, 1, len(df.columns))
            focus_tab = self.sheet.worksheet(worksheet_name)
        # if the sheet is blank
        if focus_tab.get_all_values() == []:
            # just update the sheet with the whole dataframe
            focus_tab.update(
                range_name="A1",
                values=[df.columns.values.tolist()] + df.values.tolist(),
            )
        else:
            # just add rows
            # TODO: it should put value according to the column, now it doesn't
            focus_tab.append_rows(df.values.tolist())

    def get_sheet_as_df(self, worksheet_name):
        """
        Retrieves data from a specified worksheet in the Google Sheets document as a Pandas DataFrame.

        Parameters:
            - worksheet_name (str): The name of the worksheet to retrieve data from.

        Returns:
            pandas.DataFrame or None: The retrieved data as a DataFrame.
                Returns None if the specified worksheet does not exist.

        Example:
            converter = GsToDfConverter("your_google_sheet_id")
            dataframe_from_sheet = converter.get_sheet_as_df("Sheet1")
            if dataframe_from_sheet is not None:
                # Use the retrieved DataFrame for further operations.
                print(dataframe_from_sheet)
            else:
                print("Worksheet not found.")
        """

        focus_tab = self.get_focus_tab(worksheet_name)
        if focus_tab:
            # print("Downloading sheet...", end=" ")
            sheet_data = focus_tab.get_all_values()
            # print("done")
            # print("Converting to Pandas Dataframe...", end=" ")
            df = DataFrame(sheet_data[1:], columns=sheet_data[0])
            # print("done")
            return df
        return None

    def get_col_values_as_list(self, worksheet_name, col_letter):
        """
        Retrieves values from a specified column in the Google Sheets document as a list.

        Parameters:
            - worksheet_name (str): The name of the worksheet to retrieve data from.
            - col_letter (str): The column letter (e.g., 'A', 'B', 'AA') to retrieve values from.

        Returns:
            list or None: The retrieved column values as a list.
                Returns None if the specified worksheet does not exist.

        Example:
            converter = GsToDfConverter("your_google_sheet_id")
            column_values = converter.get_col_values_as_list("Sheet1", "A")
            if column_values is not None:
                # Use the retrieved column values for further operations.
                print(column_values)
            else:
                print("Worksheet not found.")
        """
        focus_tab = self.get_focus_tab(worksheet_name)
        col_number = self.get_col_number(col_letter)
        return focus_tab.col_values(col_number) if focus_tab else None

    def get_cell_value(self, worksheet_name, cell):
        """
        Retrieves the value from a specified cell in the Google Sheets document.

        Parameters:
            - worksheet_name (str): The name of the worksheet to retrieve data from.
            - cell (str): The cell reference (e.g., 'A1', 'B2') to retrieve the value from.

        Returns:
            str or None: The retrieved cell value as a string.
                Returns None if the specified worksheet does not exist.

        Example:
            converter = GsToDfConverter("your_google_sheet_id")
            cell_value = converter.get_cell_value("Sheet1", "A1")
            if cell_value is not None:
                # Use the retrieved cell value for further operations.
                print(cell_value)
            else:
                print("Worksheet not found.")
        """
        focus_tab = self.get_focus_tab(worksheet_name)
        return focus_tab.acell(cell).value if focus_tab else None

    def get_colA_as_list(self, worksheet_name):
        return self.get_col_values_as_list(worksheet_name, "A")

    def get_A1_value(self, worksheet_name):
        return self.get_cell_value(worksheet_name, "A1")

    def get_col_number(self, col_letter):
        result = 0
        for char in col_letter:
            result = result * 26 + (ord(char) - ord("A") + 1)
        return result

    def get_focus_tab(self, worksheet_name):
        """
        Retrieves a reference to a specified worksheet in the Google Sheets document.

        Parameters:
            - worksheet_name (str): The name of the worksheet to retrieve.

        Returns:
            gspread.Worksheet or None: The retrieved worksheet reference.
                Returns None if the specified worksheet does not exist.

        Example:
            converter = GsToDfConverter("your_google_sheet_id")
            worksheet_reference = converter.get_focus_tab("Sheet1")
            if worksheet_reference is not None:
                # Use the retrieved worksheet reference for further operations.
                print(worksheet_reference.title)
            else:
                print("Worksheet not found.")
        """
        try:
            focus_tab = self.sheet.worksheet(worksheet_name)
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"There is no {worksheet_name} worksheet in the Google Sheets")
            return None
        return focus_tab
