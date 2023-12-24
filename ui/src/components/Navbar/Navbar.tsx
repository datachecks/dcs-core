import React from "react";
import styles from "./Navbar.module.css";
import { navURLs } from "../../utils/staticData";
import { BootstrapTooltip } from "../UI/BootstrapTooltip";

export const Navbar: React.FC = () => {
  return (
    <nav className={styles.navbar}>
      <div className={`datachecks_logo ${styles.logo}`} />
      <div className={styles.connects}>
        {navURLs.map((navURL, index) => (
          <BootstrapTooltip title={navURL.title}>
            <button
              aria-label={navURL.title}
              key={index}
              className={styles.connect}
              onClick={() => window.open(navURL.url)}
            >
              <div className={`${navURL.logo} ${styles.logo}`} />
            </button>
          </BootstrapTooltip>
        ))}
      </div>
    </nav>
  );
};
