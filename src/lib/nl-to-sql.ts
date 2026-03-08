import { GoogleGenAI } from '@google/genai';

const COVID_SCHEMA_CONTEXT = `
You are a SQL expert specializing in Snowflake and Covid-19 pandemic data analysis.

DATABASE: COVID19_EPIDEMIOLOGICAL_DATA
SCHEMA: PUBLIC

AVAILABLE TABLES CATEGORIZED BY DATA TYPE:

1. CORE GLOBAL TRACKING (Cases, Deaths, Population)
   - ECDC_GLOBAL (Daily): COUNTRY_REGION, CONTINENTEXP, DATE, CASES, DEATHS, CASES_SINCE_PREV_DAY, DEATHS_SINCE_PREV_DAY, POPULATION
   - JHU_COVID_19 (Daily/Status): COUNTRY_REGION, PROVINCE_STATE, DATE, CASE_TYPE ('Confirmed', 'Deaths', 'Active'), CASES, DIFFERENCE, LAT, LONG
   - WHO_TIMESERIES (Cumulative/Daily): COUNTRY_REGION, DATE, CASES, DEATHS, CASES_TOTAL, DEATHS_TOTAL, TRANSMISSION_CLASSIFICATION

2. VACCINATIONS
   - OWID_VACCINATIONS: COUNTRY_REGION, DATE, TOTAL_VACCINATIONS, PEOPLE_VACCINATED, PEOPLE_FULLY_VACCINATED, DAILY_VACCINATIONS
   - JHU_VACCINES (US Focus): PROVINCE_STATE (STABBR), DATE, DOSES_ALLOC_TOTAL, DOSES_SHIPPED_TOTAL, DOSES_ADMIN_TOTAL, PEOPLE_TOTAL (1st dose), PEOPLE_TOTAL_2ND_DOSE

3. UNITED STATES SPECIFIC
   - NYT_US_COVID19 (State/County): STATE, COUNTY, DATE, CASES, DEATHS, CASES_SINCE_PREV_DAY, DEATHS_SINCE_PREV_DAY
   - NYC_HEALTH_TESTS (NYC Zips): MODIFIED_ZCTA, DATE, COVID_CASE_COUNT, TOTAL_COVID_TESTS, PERCENT_POSITIVE
   - CDC_INPATIENT_BEDS_COVID_19 (Beds): STATE, DATE, INPATIENT_BEDS_OCCUPIED, TOTAL_INPATIENT_BEDS, INPATIENT_BEDS_IN_USE_PCT

4. MOBILITY & POLICY (Impact of measures)
   - APPLE_MOBILITY: COUNTRY_REGION, PROVINCE_STATE, DATE, TRANSPORTATION_TYPE ('walking', 'driving', 'transit'), DIFFERENCE
   - GOOG_GLOBAL_MOBILITY_REPORT: COUNTRY_REGION, PROVINCE_STATE, DATE, GROCERY_AND_PHARMACY_CHANGE_PERC, PARKS_CHANGE_PERC, RESIDENTIAL_CHANGE_PERC, RETAIL_AND_RECREATION_CHANGE_PERC, TRANSIT_STATIONS_CHANGE_PERC, WORKPLACES_CHANGE_PERC
   - CDC_POLICY_MEASURES: STATE_ID, COUNTY, DATE, POLICY_LEVEL ('State', 'County'), POLICY_TYPE, START_STOP

5. INTERNATIONAL REGIONAL DATA
   - RKI_GER_COVID19_DASHBOARD (Germany): STATE, COUNTY, CASES, DEATHS, CASES_PER_100K
   - VH_CAN_DETAILED (Canada): PROVINCE_STATE, HEALTHCARE_REGION, DATE, CASES, DEATHS
   - PCM_DPS_COVID19 (Italy): COUNTRY_REGION, PROVINCE_STATE, DATE, CASES, DEATHS, LONG, LAT
   - SCS_BE_DETAILED_* (Belgium): REGION, PROVINCE, DATE, NEW_CASES, TOTAL_IN (Hospitals), TOTAL_IN_ICU

RULES:
- Always use fully qualified table names: COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.<TABLE_NAME>
- Use Snowflake SQL syntax (ILIKE for case-insensitive, :: for casting)
- Limit results to 500 rows max unless aggregated
- Use appropriate date formatting: TO_DATE(), DATE_TRUNC()
- For time series, ORDER BY DATE
- For rankings, use ORDER BY and LIMIT
- JOINs: When joining tracking data (e.g., cases) with vaccinations, ALWAYS join on both COUNTRY_REGION and DATE.
- Negative Counts: Daily datasets often have negative values for corrections. Wrap metrics in GREATEST(0, <column>) where appropriate.
- Case Types: In JHU_COVID_19, filter by CASE_TYPE (e.g., WHERE CASE_TYPE = 'Confirmed').
`;

interface DashboardIntent {
    title: string;
    sql: string;
    explanation: string;
}

export async function naturalLanguageToSQL(question: string): Promise<{
    panels: DashboardIntent[];
    suggestedFollowUps: string[];
}> {
    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) {
        throw new Error('GEMINI_API_KEY is not configured');
    }

    const ai = new GoogleGenAI({ apiKey });

    const response = await ai.models.generateContent({
        model: 'gemini-2.0-flash',
        contents: `${COVID_SCHEMA_CONTEXT}

USER QUESTION: "${question}"

You are a Data Analyst Agent. For the user's question, break down the intent into a comprehensive dashboard with 2 to 4 distinct but related charts.
- The first chart should directly answer the core question.
- Subsequent charts should provide broader context (e.g., trend over time, comparison with neighbors, breakdowns, correlations).
- Also provide exactly 3 "suggested follow-up" questions the user could ask next to dive deeper.

Respond in EXACTLY this JSON format (no markdown, no code fences):
{
  "panels": [
    {
      "title": "Clear UI Title for Chart 1",
      "sql": "SELECT ...",
      "explanation": "Brief reasoning for this chart"
    },
    ...
  ],
  "suggestedFollowUps": ["Follow up 1?", "Follow up 2?", "Follow up 3?"]
}

Generate Snowflake SQL queries that are efficient and return meaningful results for visualizations.`,
    });

    const text = response.text?.trim() || '';

    // Parse JSON response, handling potential markdown code fences
    let cleaned = text;
    if (cleaned.startsWith('```')) {
        cleaned = cleaned.replace(/^```(?:json)?\n?/, '').replace(/\n?```$/, '');
    }

    try {
        const parsed = JSON.parse(cleaned);

        // Security validation & Fallback
        if (!parsed.panels || !Array.isArray(parsed.panels)) {
            throw new Error('Invalid JSON format: missing panels array');
        }

        const validPanels: DashboardIntent[] = [];
        for (const panel of parsed.panels) {
            if (panel.sql) {
                const sqlUpper = panel.sql.trim().toUpperCase();
                if (sqlUpper.startsWith('SELECT') || sqlUpper.startsWith('WITH')) {
                    validPanels.push(panel as DashboardIntent);
                }
            }
        }

        if (validPanels.length === 0) {
            throw new Error('No valid SELECT queries found in response');
        }

        return {
            panels: validPanels,
            suggestedFollowUps: Array.isArray(parsed.suggestedFollowUps)
                ? parsed.suggestedFollowUps.slice(0, 3)
                : [],
        };
    } catch (parseErr) {
        // Fallback: try to extract a single SQL statement from text
        const sqlMatch = text.match(/SELECT[\s\S]+?;?$/im);
        if (sqlMatch) {
            return {
                panels: [{
                    title: 'Analysis Result',
                    sql: sqlMatch[0],
                    explanation: 'Generated SQL query for: ' + question,
                }],
                suggestedFollowUps: ['Show trend over time', 'Break down by region', 'Show correlation']
            };
        }
        throw new Error('Failed to generate valid SQL from the question');
    }
}
