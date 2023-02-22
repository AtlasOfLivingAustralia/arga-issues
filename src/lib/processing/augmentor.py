import lib.processing.processingFuncs as pFuncs
import pandas as pd

class AugmentStep:
    def __init__(self, stepInfo: dict):
        self.script = stepInfo.pop("script", None)
        self.func = stepInfo.pop("function", None)
        self.args = stepInfo.pop("args", [])
        self.kwargs = stepInfo.pop("kwargs", {})

        if self.script is None:
            raise Exception("No script specified") from AttributeError
        
        if self.func is None:
            raise Exception("No function specified") from AttributeError
        
        self.augmenter = pFuncs.importFunction(self.script, self.func)

        for info in stepInfo:
            print(f"Unknown step property: {info}")
        
    def augment(self, df):
        return self.augmenter(df, *self.args, **self.kwargs)

class Augmentor:
    def __init__(self, augmentSteps: list[dict]):
        self.steps = []

        for step in augmentSteps:
            self.steps.append(AugmentStep(step.copy()))

    def augment(self, df: pd.DataFrame) -> pd.DataFrame:
        for step in self.steps:
            df = step.augment(df)

        return df
