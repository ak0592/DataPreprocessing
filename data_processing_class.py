import io
import os
from pathlib import Path
from typing import Any, Optional

import chardet
import numpy as np
import pandas as pd
from ydata_profiling import ProfileReport


class DataPreprocessingClass:
    def __init__(self, output_path: Optional[str] = None):
        """Initializes DataPreprocessingClass with the given attributes.

        Args:
            output_path (Optional[str]): データ前処理についての出力先パス. Defaults to None.
        """
        self.output_path = Path(output_path) if output_path else None
        if not self.output_path:
            print("出力先フォルダのパスが指定されていません。データの保存がされません。")

    @staticmethod
    def check_file_codec(input_file_path: str) -> None:
        """指定したデータファイルの文字コードを確認する

        Args:
            input_file_path (str): 対象のデータファイルパス
        """
        # ファイルをバイナリモードで開く
        with open(input_file_path, 'rb') as file:
            data = file.read()
        # 文字コードを判定
        result = chardet.detect(data)
        # 判定結果を表示
        print(f"判定された文字コード: {result['encoding']}")

    def save_dataframe(self, output_df: pd.DataFrame, output_filename: str) -> None:
        """pd.DataFrameを保存する

        Args:
            output_filename (str): 保存するファイル名
        """
        if not self.output_path:
            print("出力先フォルダのパスが指定されていません。データの保存がされません。")

        ext = output_filename.split(".")[1]
        if ext == "csv":
            output_df.to_csv(
                self.output_path / output_filename, encoding="utf_8_sig"
            )
        elif ext == "pkl" or ext == "pickle":
            output_df.to_pickle(self.output_path / output_filename)
        elif ext == "xlsx":
            output_df.to_excel(
                self.output_path / output_filename, engine="openpyxl"
            )
        else:
            raise ValueError(f"拡張子={ext}は対応していません。csv, pkl, xlsxから選んでください。")

        print(f"結果を{output_filename}にcsvファイルとして保存しています。")

    def transform_to_dataframe(self, input_data: Any, output_filename: Optional[str] = None) -> pd.DataFrame:
        """入力データをpd.DataFrameクラスに変換する

        Args:
            input_data (Any): 入力データ, pd.Series, np.ndarray, listのみ対応
            output_filename(Optional[str], optional): 出力するファイル名. Defaults to None.

        Returns:
            pd.DataFrame: 変換後データ
        """
        def count_depth(lst: Any, level: int = 1, func: function = max) -> int:
            """listのネスト階層をカウントする

            Args:
                lst (Any): 確認するオブジェクト
                level (int, optional): 階層数. Defaults to 1.
                func (function, optional): ネストの最大値か最小値. Defaults to max.

            Returns:
                int: ネスト数
            """
            if not isinstance(lst, list) or not lst:

                return level

            return func(count_depth(item, level + 1) for item in lst)

        if isinstance(input_data, pd.DataFrame):
            return input_data
        if isinstance(input_data, pd.Series):
            output_df = pd.DataFrame(input_data)
        elif isinstance(input_data, list):
            # ネストの最大値を確認
            n_max_next = count_depth(input_data, func=max)
            # ネストの最小値を確認
            n_min_next = count_depth(input_data, func=min)
            # ネストの最大値と最小値が一致しない場合、入力データのエラーとする
            if n_max_next != n_min_next:
                raise ValueError("入力データのリストの構造が不均一の可能性があります。確認して下さい。")
            # ネストの階層が1の場合
            if n_max_next == 1:
                output_df = pd.DataFrame(input_data, columns=["col_0"])
            elif n_max_next == 2:
                output_df = pd.DataFrame(
                    input_data, columns=["col_0", "col_1"])
            else:
                raise ValueError("入力データは3次元以上のデータであり、対応していません。")
        elif isinstance(input_data, np.ndarray):
            input_data_shape = input_data.shape
            if len(input_data_shape) == 1:
                output_df = pd.DataFrame(input_data, columns=["col_0"])
            elif len(input_data_shape) == 2:
                output_df = pd.DataFrame(
                    input_data, columns=["col_0", "col_1"]
                )
            else:
                raise ValueError("入力データは3次元以上のデータであり、対応していません。")

        # 結果の保存
        if output_filename:
            self.save_dataframe(
                output_df=output_df,
                output_filename=output_filename
            )

        return output_df

    def describe_info(self, input_df: pd.DataFrame, output_filename: Optional[str] = None) -> None:
        """入力データの中身をexcelファイルに出力する

        Args:
            input_df (pd.DataFrame): 入力データ
            output_filename(Optional[str], optional): 出力するファイル名. Defaults to None.
        """
        # print(input_df.info())

        if output_filename:
            # infoの出力をキャプチャ
            buffer = io.StringIO()
            input_df.info(buf=buffer, verbose=True)
            info_str = buffer.getvalue()
            # 出力ファイルは.txtに統一する
            output_base_filename = os.path.splitext(output_filename)[0]
            output_txt_filename = output_base_filename + ".txt"
            # ファイルに書き込む
            with open(self.output_path / output_txt_filename, 'w') as f:
                f.write(info_str)
            print(f"結果を{output_txt_filename}にtxtファイルとして保存しています。")

    def create_profile(self, input_df: pd.DataFrame, output_filename: str, title: Optional[str] = None):
        """入力データのProfile HTMLを作成する

        Args:
            input_df (pd.DataFrame): 入力データ
            output_filename (str): htmlの出力先パス
            title (Optional[str], optional): Profileのタイトル. Defaults to None.
        """
        # インスタンスの作成
        if not title:
            title = os.path.basename(output_filename).split(".")[0]
        profile = ProfileReport(input_df, title=title)
        profile.to_file(self.output_path / output_filename)
        print(f"結果を{output_filename}にhtmlファイルとして保存しています。")
