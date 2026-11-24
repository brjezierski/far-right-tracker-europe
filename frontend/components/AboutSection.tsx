import styles from "./AboutSection.module.css";

export default function AboutSection() {
  return (
    <section className={styles.about}>
      <h2>Methodology</h2>
      <p>
        <strong>Polling data.</strong> The polls come from Wikipedia’s
        country-specific “Opinion polling for the … election” pages, since it is
        the most extensive open-source data source and its structure is
        consistent enough to scrape across countries. A Python scraper
        (requests, BeautifulSoup and pandas) pulls the tables and parses the
        dates, skipping election-result rows and some alternative-scenario
        tables. A GitHub Actions workflow refreshes the data daily. The scraper
        collects polls for all parties, not just far-right ones. The chart line
        is a 5-poll rolling average, interpolated to one value per day. The
        number on the map is the average, over the three most recent poll dates,
        of the combined support for all parties classified as far-right.
      </p>
      <p>
        <strong>Classifying a party as far-right.</strong> The classifier
        doesn’t judge parties itself. For each party it takes the “political
        position” and “ideology” fields from the infobox of its English
        Wikipedia article. It counts the party as far-right if either field
        contains “far-right” or “national conservatism”. I treat national
        conservatism as far-right here because it often espouses a state that
        excludes all but a narrow group, which tends to result in attacks on
        civil society, such as LGBT rights groups and women’s rights
        organisations, and makes the state more illiberal.
      </p>
      <p>
        The label depends on how Wikipedia editors describe a party. As a
        result, some parties that Wikipedia does not call far-right are counted
        (for example Reform UK, Turkey’s AKP and Georgian Dream), while ruling
        parties described as “big tent”, such as Serbia’s SNS, are not. In
        Georgia, Turkey and Kosovo, the country figure is driven mostly by one
        or two such parties.
      </p>
      <h3>Notes</h3>
      <ul>
        <li>
          Parliamentary election polls are used for all countries with the
          exception of France. For France I mapped presidential candidates to
          their parties and displayed the support under the party name.
          Presidential polls are used because the presidential race is the
          central contest in France, and its presidential polling is far more
          frequent and up to date than its parliamentary polling.
        </li>
        <li>
          Russia was not included since polling there is unreliable due to
          intimidation and the system itself is not democratic. Belarus was also
          excluded for the latter reason. Bosnia and Herzegovina had limited
          polling data online.
        </li>
      </ul>
    </section>
  );
}
