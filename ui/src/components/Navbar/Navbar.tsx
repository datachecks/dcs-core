import React from "react";
import styles from "./Navbar.module.css";
import { navURLs } from "../../types/component.type";

function Navbar() {
  return (
    <div className={styles.navbar}>
      <div className={`datachecks_logo ${styles.logo}`} />
      <div className={styles.connects}>
        {navURLs.map((navURL, index) => (
          <div
            key={index}
            className={styles.connect}
            onClick={() => window.open(navURL.url)}
          >
            <div className={`${navURL.logo} ${styles.logo}`} />
            {navURL.title}
          </div>
        ))}
      </div>
    </div>
  );
}

export default Navbar;
