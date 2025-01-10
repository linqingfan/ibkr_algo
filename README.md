# ibkr_algo
Provides a template to read a list of assets saved in excel and process

# Excel file field
Reference to https://interactivebrokers.github.io/tws-api/historical_bars.html:

barsize: time frame, e.g. 30 min, hourly daily chart etc,  <br>
<img src="images/ValidBarSizes.png" alt="Valid Bar Sizes" width="400" height="200"> <br>
duration: how many historical bars to read from the current time e.g. 5 days, 1 Y etc  <br>
<img src="images/ValidDurations.png" alt="Valid Durations" width="400" height="200"> <br>
history_end_dt: this is not use currently as it always end at current time <br>
whatToShow: <br>
<img src="images/WhatToShow.png" alt="What to show definition" width="800" height="200"> <br>
<img src="images/whattoshow2.png" alt="What to show definition" width="1000" height="200"> <br>

