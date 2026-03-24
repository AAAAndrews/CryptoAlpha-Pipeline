"""
Data preprocessing module: used to load data and preprocess data, such as data cleaning, data conversion, data normalization, etc.
Support custom data loaders, such as CSVDataLoader, JSONDataLoader, etc.
Support data loader embedded processors, such as MultiHandler, for processing data loaded by data loaders
"""
import pandas as pd
import numpy as np

class BaseDataLoader:
    """
    Data loader base class. All custom data loaders need to inherit and implement the receive and compile methods.
    """
    def __init__(self):
        self.content = None

    def receive(self, **kwargs):
        raise NotImplementedError("Subclasses must implement this method")

    def compile(self, **kwargs):
        raise NotImplementedError("Subclasses must implement this method")

    @property
    def dataset(self):
        if self.content is None:
            self.content = self.receive()
        return self.compile()


class CSVDataLoader(BaseDataLoader):
    """
    CSV File data loader.
    """
    def __init__(self, file_path, timestamp_col='open_time'):
        super().__init__()
        self.file_path = file_path
        self.timestamp_col = timestamp_col

    def receive(self, **kwargs):
        try:
            self.content = pd.read_csv(self.file_path, encoding='utf-8')
            if self.timestamp_col not in self.content.columns:
                raise ValueError(f"CSV file must contain a '{self.timestamp_col}' column")
            return self.content
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return None

    def compile(self, **kwargs):
        if self.content is None:
            raise ValueError("No data received. Please call receive first.")
        return self.content


class DataFrameLoader(BaseDataLoader):
    """
    Data loader for loading DataFrame directly.
    """
    def __init__(self, dataframe,timestamp_col='open_time'):
        super().__init__()
        self.dataframe = dataframe
        self.timestamp_col = timestamp_col

    def receive(self, **kwargs):
        if not isinstance(self.dataframe, pd.DataFrame):
            raise ValueError("Input is not a DataFrame")
        if self.timestamp_col not in self.dataframe.columns:
            raise ValueError(f"DataFrame must contain a '{self.timestamp_col}' column")
        self.content = self.dataframe
        return self.content

    def compile(self, **kwargs):
        if self.content is None:
            raise ValueError("No data received. Please call receive first.")
        return self.content


class MultiAssetDataHandler:
    """
    Multi-standard data processor. Responsible for integrating multiple data sources and outputting three-dimensional arrays and yield matrices.
    """
    def __init__(self, context, multi=True):
        if context is None:
            raise ValueError("Context is required")
        self.context = context
        self.multi = multi
        self.loaders = []
        self.dataframes = []

    def add_loader(self, loader_class, sources,timestamp_col='open_time', tag_col="symbol"):
        """
        Add multiple data loader instances.
        :param loader_class: Data loader classes (such as CSVDataLoader).
        :param sources: List of file paths or list of DataFrames.
        """
        for src in sources:
            loader = loader_class(src, timestamp_col=timestamp_col)
            df = loader.dataset
            if df is not None:
                if tag_col not in df.columns:
                    df[tag_col] = sources.index(src) + 1
                self.loaders.append(loader)
                self.dataframes.append(df)
            else:
                print(f"Failed to load data from {src}")

    def to_3d_array(self, fields=('open', 'high', 'low', 'close'), timestamp_col='open_time',tag_col="symbol", period=1, update_mode=False):
        """
        Integrate all data and output a three-dimensional array and yield matrix.
        :param fields: List of characteristic fields.
        :param period: yield cycle.
        :param update_mode: Whether it is in update mode.
        :return: data_3d, fields, stocks, dates, returns_matrix, context
        Schematic diagram of three-dimensional array structure:

            /
           /
          timestamps(2)
         /
        /-------symbols(1)-----
        |
        |
        fields(0)
        |
        |
        """
        if not self.dataframes:
            raise ValueError("No data loaded. Please call add_loader first.")

        self.context.params['fields'] = list(fields)
        all_data = pd.concat(self.dataframes, axis=0).sort_values([timestamp_col, tag_col])
        symbols = all_data[tag_col].unique()
        timestamps = all_data[timestamp_col].unique()

        data_3d = np.zeros((len(fields), len(symbols), len(timestamps)))
        returns_matrix = np.full((len(timestamps), len(symbols)), np.nan)

        for j, symbol in enumerate(symbols):
            single_symbol_data = all_data[all_data[tag_col] == symbol].set_index(timestamp_col).reindex(timestamps)

            for i, field in enumerate(fields):
                data_3d[i, j, :] = single_symbol_data[field].values
                
            if period > 0 and "open" in single_symbol_data.columns:
                future_opens = single_symbol_data['open'].shift(-period-1)
                current_opens = single_symbol_data['open'].shift(-1)
                returns = (future_opens - current_opens) / current_opens
                returns_matrix[:, j] = returns.values

        if not update_mode:
            # Intercept valid intervals and remove invalid data caused by shift
            valid_len = len(timestamps) - period - 1
            data_3d = data_3d[:, :, :valid_len]
            returns_matrix = returns_matrix[:valid_len, :]
            timestamps = timestamps[:valid_len]

        return data_3d, list(fields), symbols, timestamps, returns_matrix, self.context







