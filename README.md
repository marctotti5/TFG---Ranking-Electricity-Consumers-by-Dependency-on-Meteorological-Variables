# TFG---Ranking-Electricity-Consumers-by-Dependency-on-Meteorological-Variables

In this project we will work with the London Household Smart Meter dataset (https://data.london.gov.uk/dataset/smartmeter-energy-use-data-in-london-households) available at Smart meters in London | Kaggle, which includes energy consumption readings for a sample of 5,567 London Households between November 2011 and February 2014. We will extend this dataset by incorporating exogenous variables, such as meteorological and calendar effects since, for example, it is well known the (non-linear) relationship between electricity consumption and temperature.

**The main objective of this thesis project is the development a ranking of consumers considering the dependency of their consumption on different meteorological variables such as temperature and relative humidity.**

The relationship between energy consumption and temperature is clear in the aggregate consumption of a group of consumers but when we analyze a single consumer the relationship is more diffuse since not all consumers react in the same way to a change in temperature and, of course, it depends on whether the customer is or is not at home. 

In this project, firstly, we will develop a tool to classify whether there is activity within the household based on the time series of consumption. Next, the dependence between consumption and meteorological variables will be studied in the periods where there is presence / activity within the home. The ranking of customers will be made up according to dependency values, that is, the higher the dependency, the higher the ranking.

We will use and generalize the approach developed by Alonso and Peña (2019) for time series clustering dependency. This approach is based on linear dependency measures but can be generalized to consider monotonic dependency.

