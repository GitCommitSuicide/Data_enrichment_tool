def build_queries(company: str) -> list[str]:
    c = company.strip()

    return [
        f'{c} official website or About us page of {c}',
        # 🔥 Core company overview (identity + services + market)
        f'"{c}" company overview what does it do products services customers',

        # 🔥 Structured profiles (high-quality sources)
        f'"{c}" site:linkedin.com OR site:crunchbase.com OR site:bloomberg.com company profile',

        # 🔥 Leadership + decision makers
        f'"{c}" CEO founder leadership team executives directors and ones on higher positions',

        # 🔥 Financials + size (merged into one strong query)
        f'"{c}" revenue funding valuation employee count headcount',

        # 🔥 Market + competitors + positioning
        f'"{c}" competitors alternatives target market industries customers',

        # 🔥 Legal + registration (global coverage)
        f'"{c}" company registration incorporation number CIN MCA UK Companies House',

        # 🔥 Reviews + reputation + partnerships
        f'"{c}" reviews rating trustpilot glassdoor partnerships awards certifications'
    ]
