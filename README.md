# UGAHacks2024
Hackathon project for UGA Hacks 2024
## Inspiration ✨
Well, for context, per the challenge description, "1000 Children go missing every day in the United States, and 1 in 6 become victims of human trafficking". We wanted to put our skills to work and help inform people about the dangers associated with each county.
## What it does 🛠️
This workunit outputs 3 important tables. 
1) The first table (Children_Missing_Per_County) associates the number of missing children to every county fip.
2) The second table (Rates) takes all of the county fips and creates a table that associates the unemployment rates, lack of education, poverty rates, and population to each county fip.
3) The third table (County_Risk) associates the risk of missing children to each county fip.
## How we built it ⚙️
The workunit was built 100% using ecl. 
Children_Missing_Per_County was made using a cross tabulation function provided.
Rates is data that was combined from datasets that included unemployment rates, lack of education, poverty rates, and population.
County_Risk was built implementing a formula where we summed the normalization of Rates divided by their correlation constant.
## Challenges we ran into
The process to building these datasets began slowly and then took off like superman. ECL is hard syntactically so we mostly struggled with that in the beginning; however, once we figured out how to properly use joins and transforms we were able to fly.
## Accomplishments that we're proud of
We are astounded with how much ECL we were able to learn in just one weekend. We are proud to have built meaningful datasets which 1) prove correlation and 2) can help warn people about risks.
## What's next for County Correlation Risk Assessment
We have the layout for a website thanks to our awesome UX designer, so all that's left is for us to build the ROXIE Queries and then add requesting functionality to the website. This would allow users to look up their county or a county they are planning on travelling to and assess the risk missing children.
