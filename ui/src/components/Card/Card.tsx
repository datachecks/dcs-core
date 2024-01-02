import React from "react";
import styles from "./Card.module.css";

export const Card = React.forwardRef<
  HTMLDivElement,
  React.HTMLAttributes<HTMLDivElement>
>((props, ref) => (
  <div ref={ref} className={styles.card} {...props}>
    {props.children}
  </div>
));

export const CardHeader = React.forwardRef<
  HTMLDivElement,
  React.HTMLAttributes<HTMLDivElement>
>((props, ref) => (
  <div ref={ref} className={styles.cardHeader} {...props}>
    {props.children}
  </div>
));

export const CardTitle = React.forwardRef<
  HTMLDivElement,
  React.HTMLAttributes<HTMLDivElement>
>((props, ref) => (
  <div ref={ref} className={styles.cardTitle} {...props}>
    {props.children}
  </div>
));

export const CardSubtitle = React.forwardRef<
  HTMLDivElement,
  React.HTMLAttributes<HTMLDivElement>
>((props, ref) => (
  <div ref={ref} className={styles.cardSubtitle} {...props}>
    {props.children}
  </div>
));

export const CardDescription = React.forwardRef<
  HTMLDivElement,
  React.HTMLAttributes<HTMLDivElement>
>((props, ref) => (
  <div ref={ref} className={styles.cardDescription} {...props}>
    {props.children}
  </div>
));

export const CardContent = React.forwardRef<
  HTMLDivElement,
  React.HTMLAttributes<HTMLDivElement>
>((props, ref) => (
  <div ref={ref} className={styles.cardContent} {...props}>
    {props.children}
  </div>
));

export const CardSection = React.forwardRef<
  HTMLDivElement,
  React.HTMLAttributes<HTMLDivElement>
>((props, ref) => (
  <div ref={ref} className={styles.cardSection} {...props}>
    {props.children}
  </div>
));
