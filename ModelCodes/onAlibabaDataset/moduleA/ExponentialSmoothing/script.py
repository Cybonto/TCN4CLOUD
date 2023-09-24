# This file has been autogenerated by version 1.53.0 of the Azure Automated Machine Learning SDK.


import numpy
import numpy as np
import pandas as pd
import pickle
import argparse


# For information on AzureML packages: https://docs.microsoft.com/en-us/python/api/?view=azure-ml-py
from azureml.training.tabular._diagnostics import logging_utilities


def setup_instrumentation(automl_run_id):
    import logging
    import sys

    from azureml.core import Run
    from azureml.telemetry import INSTRUMENTATION_KEY, get_telemetry_log_handler
    from azureml.telemetry._telemetry_formatter import ExceptionFormatter

    logger = logging.getLogger("azureml.training.tabular")

    try:
        logger.setLevel(logging.INFO)

        # Add logging to STDOUT
        stdout_handler = logging.StreamHandler(sys.stdout)
        logger.addHandler(stdout_handler)

        # Add telemetry logging with formatter to strip identifying info
        telemetry_handler = get_telemetry_log_handler(
            instrumentation_key=INSTRUMENTATION_KEY, component_name="azureml.training.tabular"
        )
        telemetry_handler.setFormatter(ExceptionFormatter())
        logger.addHandler(telemetry_handler)

        # Attach run IDs to logging info for correlation if running inside AzureML
        try:
            run = Run.get_context()
            return logging.LoggerAdapter(logger, extra={
                "properties": {
                    "codegen_run_id": run.id,
                    "automl_run_id": automl_run_id
                }
            })
        except Exception:
            pass
    except Exception:
        pass

    return logger


automl_run_id = 'AutoML_d08a83a0-f8e5-4461-8ab6-4b4e5e6493c7_0'
logger = setup_instrumentation(automl_run_id)


def split_dataset(X, y, weights, split_ratio, should_stratify):
    '''
    Splits the dataset into a training and testing set.

    Splits the dataset using the given split ratio. The default ratio given is 0.25 but can be
    changed in the main function. If should_stratify is true the data will be split in a stratified
    way, meaning that each new set will have the same distribution of the target value as the
    original dataset. should_stratify is true for a classification run, false otherwise.
    '''
    from sklearn.model_selection import train_test_split

    random_state = 42
    if should_stratify:
        stratify = y
    else:
        stratify = None

    if weights is not None:
        X_train, X_test, y_train, y_test, weights_train, weights_test = train_test_split(
            X, y, weights, stratify=stratify, test_size=split_ratio, random_state=random_state
        )
    else:
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, stratify=stratify, test_size=split_ratio, random_state=random_state
        )
        weights_train, weights_test = None, None

    return (X_train, y_train, weights_train), (X_test, y_test, weights_test)


def get_training_dataset(dataset_id):
    '''
    Loads the previously used dataset.
    
    It assumes that the script is run in an AzureML command job under the same workspace as the original experiment.
    '''
    
    from azureml.core.dataset import Dataset
    from azureml.core.run import Run
    
    logger.info("Running get_training_dataset")
    ws = Run.get_context().experiment.workspace
    dataset = Dataset.get_by_id(workspace=ws, id=dataset_id)
    return dataset.to_pandas_dataframe()


def prepare_data(dataframe):
    '''
    Prepares data for training.
    
    Cleans the data, splits out the feature and sample weight columns and prepares the data for use in training.
    This function can vary depending on the type of dataset and the experiment task type: classification,
    regression, or time-series forecasting.
    '''
    
    from azureml.training.tabular.preprocessing import data_cleaning
    
    logger.info("Running prepare_data")
    label_column_name = 'func_ID'
    
    # extract the features, target and sample weight arrays
    y = dataframe[label_column_name].values
    X = dataframe.drop([label_column_name], axis=1)
    sample_weights = None
    X, y, sample_weights = data_cleaning._remove_nan_rows_in_X_y(X, y, sample_weights,
     is_timeseries=True, target_column=label_column_name)
    
    return X, y, sample_weights


def generate_data_transformation_config():
    from azureml.training.tabular.featurization._featurization_config import FeaturizationConfig
    from azureml.training.tabular.featurization.timeseries.category_binarizer import CategoryBinarizer
    from azureml.training.tabular.featurization.timeseries.missingdummies_transformer import MissingDummiesTransformer
    from azureml.training.tabular.featurization.timeseries.numericalize_transformer import NumericalizeTransformer
    from azureml.training.tabular.featurization.timeseries.restore_dtypes_transformer import RestoreDtypesTransformer
    from azureml.training.tabular.featurization.timeseries.short_grain_dropper import ShortGrainDropper
    from azureml.training.tabular.featurization.timeseries.time_index_featurizer import TimeIndexFeaturizer
    from azureml.training.tabular.featurization.timeseries.time_series_imputer import TimeSeriesImputer
    from azureml.training.tabular.featurization.timeseries.timeseries_transformer import TimeSeriesPipelineType
    from azureml.training.tabular.featurization.timeseries.timeseries_transformer import TimeSeriesTransformer
    from azureml.training.tabular.featurization.timeseries.unique_target_grain_dropper import UniqueTargetGrainDropper
    from collections import OrderedDict
    from numpy import dtype
    from numpy import nan
    from pandas.core.indexes.base import Index
    from sklearn.pipeline import Pipeline
    
    transformer_list = []
    transformer1 = UniqueTargetGrainDropper(
        cv_step_size=800,
        max_horizon=300,
        n_cross_validations=5,
        target_lags=[0],
        target_rolling_window_size=0
    )
    transformer_list.append(('unique_target_grain_dropper', transformer1))
    
    transformer2 = MissingDummiesTransformer(
        numerical_columns=['__time__', 'latency']
    )
    transformer_list.append(('make_numeric_na_dummies', transformer2))
    
    transformer3 = TimeSeriesImputer(
        end=None,
        freq='N',
        impute_by_horizon=False,
        input_column=['__time__', 'latency'],
        limit=None,
        limit_direction='forward',
        method=OrderedDict([('ffill', [])]),
        option='fillna',
        order=None,
        origin=None,
        value={'__time__': 45189.5, 'latency': 2002.096971}
    )
    transformer_list.append(('impute_na_numeric_datetime', transformer3))
    
    transformer4 = ShortGrainDropper(
        cv_step_size=800,
        max_horizon=300,
        n_cross_validations=5,
        target_lags=[0],
        target_rolling_window_size=0
    )
    transformer_list.append(('grain_dropper', transformer4))
    
    transformer5 = RestoreDtypesTransformer(
        dtypes={'runtime': dtype('O'), '_automl_target_col': dtype('int64'), '__time__': dtype('int64'), 'latency': dtype('float64')},
        target_column='_automl_target_col'
    )
    transformer_list.append(('restore_dtypes_transform', transformer5))
    
    transformer6 = NumericalizeTransformer(
        categories_by_col={'runtime': Index(['custom', 'custom-container', 'debian8', 'dotnetcore2.1', 'golang1.8', 'java11', 'java8', 'nodejs10',
               'nodejs12', 'nodejs4.4', 'nodejs6', 'nodejs8', 'php7.2', 'python2.7', 'python3'],
              dtype='object')},
        exclude_columns=set(),
        include_columns=set()
    )
    transformer_list.append(('make_categoricals_numeric', transformer6))
    
    transformer7 = TimeIndexFeaturizer(
        correlation_cutoff=0.99,
        country_or_region=None,
        datetime_columns=None,
        force_feature_list=None,
        freq='N',
        holiday_end_time=None,
        holiday_start_time=None,
        overwrite_columns=True,
        prune_features=True
    )
    transformer_list.append(('make_time_index_featuers', transformer7))
    
    transformer8 = CategoryBinarizer(
        columns=[],
        drop_first=False,
        dummy_na=False,
        encode_all_categoricals=False,
        prefix=None,
        prefix_sep='_'
    )
    transformer_list.append(('make_categoricals_onehot', transformer8))
    
    pipeline = Pipeline(steps=transformer_list)
    tst = TimeSeriesTransformer(
        country_or_region=None,
        drop_column_names=[],
        featurization_config=FeaturizationConfig(
            blocked_transformers=None,
            column_purposes=None,
            dataset_language=None,
            prediction_transform_type=None,
            transformer_params=None
        ),
        force_time_index_features=None,
        freq='N',
        grain_column_names=None,
        group=None,
        lookback_features_removed=False,
        max_horizon=300,
        origin_time_colname='origin',
        pipeline=pipeline,
        pipeline_type=TimeSeriesPipelineType.FULL,
        seasonality=1,
        time_column_name='idx',
        time_index_non_holiday_features=[],
        use_stl=None
    )
    
    return tst
    
    
def generate_algorithm_config():
    '''
    Specifies the actual algorithm and hyperparameters for training the model.
    
    It is the last stage of the final scikit-learn pipeline. For ensemble models, generate_preprocessor_config_N()
    (if needed) and generate_algorithm_config_N() are defined for each learner in the ensemble model,
    where N represents the placement of each learner in the ensemble model's list. For stack ensemble
    models, the meta learner generate_algorithm_config_meta() is defined.
    '''
    from azureml.training.tabular.models._timeseries._exponential_smoothing import ExponentialSmoothing
    from numpy import array
    
    algorithm = ExponentialSmoothing(
        timeseries_param_dict={'time_column_name': 'idx', 'grain_column_names': None, 'target_column_name': 'func_ID', 'drop_column_names': [], 'overwrite_columns': True, 'dropna': False, 'transform_dictionary': {'min': '_automl_target_col', 'max': '_automl_target_col', 'mean': '_automl_target_col'}, 'max_horizon': 300, 'origin_time_colname': 'origin', 'country_or_region': None, 'n_cross_validations': 5, 'short_series_handling': True, 'max_cores_per_iteration': -1, 'feature_lags': None, 'target_aggregation_function': None, 'cv_step_size': 800, 'iteration_timeout_minutes': 360, 'seasonality': 1, 'use_stl': None, 'freq': 'N', 'short_series_handling_configuration': 'auto', 'target_lags': [0], 'target_rolling_window_size': 0, 'arimax_raw_columns': ['latency', 'runtime', '__time__', 'idx']}
    )
    
    return algorithm
    
    
def build_model_pipeline():
    '''
    Defines the scikit-learn pipeline steps.
    
    For time-series forecasting models, the scikit-learn pipeline is wrapped in a ForecastingPipelineWrapper,
    which has some additional logic needed to properly handle time-series data depending on the applied algorithm.
    '''
    from azureml.training.tabular.models.forecasting_pipeline_wrapper import ForecastingPipelineWrapper
    from sklearn.pipeline import Pipeline
    
    logger.info("Running build_model_pipeline")
    pipeline = Pipeline(
        steps=[
            ('tst', generate_data_transformation_config()),
            ('model', generate_algorithm_config())
        ]
    )
    forecast_pipeline_wrapper = ForecastingPipelineWrapper(pipeline, stddev=[6227.9087610957])
    
    return forecast_pipeline_wrapper


def train_model(X, y, sample_weights=None, transformer=None):
    '''
    Calls the fit() method to train the model.
    
    The return value is the model fitted/trained on the input data.
    '''
    
    logger.info("Running train_model")
    model_pipeline = build_model_pipeline()
    
    model = model_pipeline.fit(X, y)
    return model


def calculate_metrics(model, X, y, sample_weights, X_test, y_test, cv_splits=None):
    '''
    Calculates the metrics that can be used to evaluate the model's performance.
    
    Metrics calculated vary depending on the experiment type. Classification, regression and time-series
    forecasting jobs each have their own set of metrics that are calculated.'''
    
    from azureml.training.tabular.preprocessing._dataset_binning import get_dataset_bins
    from azureml.training.tabular.score.scoring import score_forecasting
    from azureml.training.tabular.score.scoring import score_regression
    
    y_pred, _ = model.forecast(X_test)
    y_min = np.min(y)
    y_max = np.max(y)
    y_std = np.std(y)
    
    bin_info = get_dataset_bins(cv_splits, X, None, y)
    regression_metrics_names, forecasting_metrics_names = get_metrics_names()
    metrics = score_regression(
        y_test, y_pred, regression_metrics_names, y_max, y_min, y_std, sample_weights, bin_info)
    
    try:
        horizons = X_test['horizon_origin'].values
    except Exception:
        # If no horizon is present we are doing a basic forecast.
        # The model's error estimation will be based on the overall
        # stddev of the errors, multiplied by a factor of the horizon.
        horizons = np.repeat(None, y_pred.shape[0])
    
    featurization_step = generate_data_transformation_config()
    grain_column_names = featurization_step.grain_column_names
    time_column_name = featurization_step.time_column_name
    
    forecasting_metrics = score_forecasting(
        y_test, y_pred, forecasting_metrics_names, horizons, y_max, y_min, y_std, sample_weights, bin_info,
        X_test, X, y, grain_column_names, time_column_name)
    metrics.update(forecasting_metrics)
    return metrics


def get_metrics_names():
    
    regression_metrics_names = [
        'median_absolute_error',
        'root_mean_squared_error',
        'mean_absolute_percentage_error',
        'explained_variance',
        'mean_absolute_error',
        'r2_score',
        'spearman_correlation',
        'root_mean_squared_log_error',
        'residuals',
        'predicted_true',
    ]
    forecasting_metrics_names = [
        'forecast_mean_absolute_percentage_error',
        'forecast_residuals',
        'forecast_table',
        'forecast_adjustment_residuals',
    ]
    return regression_metrics_names, forecasting_metrics_names


def get_metrics_log_methods():
    
    metrics_log_methods = {
        'median_absolute_error': 'log',
        'forecast_residuals': 'Skip',
        'forecast_table': 'Skip',
        'root_mean_squared_error': 'log',
        'mean_absolute_percentage_error': 'log',
        'explained_variance': 'log',
        'mean_absolute_error': 'log',
        'r2_score': 'log',
        'forecast_adjustment_residuals': 'Skip',
        'spearman_correlation': 'log',
        'forecast_mean_absolute_percentage_error': 'Skip',
        'root_mean_squared_log_error': 'log',
        'residuals': 'log_residuals',
        'predicted_true': 'log_predictions',
    }
    return metrics_log_methods


def main(training_dataset_id=None):
    '''
    Runs all functions defined above.
    '''
    
    from azureml.automl.core.inference import inference
    from azureml.core.run import Run
    from azureml.training.tabular.score._cv_splits import _CVSplits
    from azureml.training.tabular.score.scoring import aggregate_scores
    
    import mlflow
    
    # The following code is for when running this code as part of an AzureML script run.
    run = Run.get_context()
    
    df = get_training_dataset(training_dataset_id)
    X, y, sample_weights = prepare_data(df)
    tst = generate_data_transformation_config()
    tst.fit(X, y)
    ts_param_dict = tst.parameters
    short_series_dropper = next((step for key, step in tst.pipeline.steps if key == 'grain_dropper'), None)
    if short_series_dropper is not None and short_series_dropper.has_short_grains_in_train and grains is not None and len(grains) > 0:
        # Preprocess X so that it will not contain the short grains.
        dfs = []
        X['_automl_target_col'] = y
        for grain, df in X.groupby(grains):
            if grain in short_series_processor.grains_to_keep:
                dfs.append(df)
        X = pd.concat(dfs)
        y = X.pop('_automl_target_col').values
        del dfs
    cv_splits = _CVSplits(X, y, frac_valid=None, CV=5, n_step=800, is_time_series=True, task='regression', timeseries_param_dict=ts_param_dict)
    scores = []
    for X_train, y_train, sample_weights_train, X_valid, y_valid, sample_weights_valid in cv_splits.apply_CV_splits(X, y, sample_weights):
        partially_fitted_model = train_model(X_train, y_train, transformer=tst)
        metrics = calculate_metrics(partially_fitted_model, X, y, sample_weights, X_test=X_valid, y_test=y_valid, cv_splits=cv_splits)
        scores.append(metrics)
        print(metrics)
    model = train_model(X_train, y_train, transformer=tst)
    
    metrics = aggregate_scores(scores)
    metrics_log_methods = get_metrics_log_methods()
    print(metrics)
    for metric in metrics:
        if metrics_log_methods[metric] == 'None':
            logger.warning("Unsupported non-scalar metric {}. Will not log.".format(metric))
        elif metrics_log_methods[metric] == 'Skip':
            pass # Forecasting non-scalar metrics and unsupported classification metrics are not logged
        else:
            getattr(run, metrics_log_methods[metric])(metric, metrics[metric])
    cd = inference.get_conda_deps_as_dict(True)
    
    # Saving ML model to outputs/.
    signature = mlflow.models.signature.infer_signature(X, y)
    mlflow.sklearn.log_model(
        sk_model=model,
        artifact_path='outputs/',
        conda_env=cd,
        signature=signature,
        serialization_format=mlflow.sklearn.SERIALIZATION_FORMAT_PICKLE)
    
    run.upload_folder('outputs/', 'outputs/')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--training_dataset_id', type=str, default='ee6ba2d5-7e9e-4518-b0c6-17dc05098a19',     help='Default training dataset id is populated from the parent run')
    args = parser.parse_args()
    
    try:
        main(args.training_dataset_id)
    except Exception as e:
        logging_utilities.log_traceback(e, logger)
        raise