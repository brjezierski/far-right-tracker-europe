"use client";
import { useState, useEffect, useRef } from "react";
import styles from "./TimelineSlider.module.css";

type TimelineSliderProps = {
  startDate: Date;
  endDate: Date;
  currentDate: Date;
  onDateChange: (date: Date) => void;
  isPlaying: boolean;
  onPlayPause: () => void;
};

export default function TimelineSlider({
  startDate,
  endDate,
  currentDate,
  onDateChange,
  isPlaying,
  onPlayPause,
}: TimelineSliderProps) {
  const totalDays = Math.floor(
    (endDate.getTime() - startDate.getTime()) / (1000 * 60 * 60 * 24)
  );
  const currentDays = Math.floor(
    (currentDate.getTime() - startDate.getTime()) / (1000 * 60 * 60 * 24)
  );
  const percentage = totalDays > 0 ? (currentDays / totalDays) * 100 : 0;

  const handleSliderChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = parseInt(e.target.value);
    const newDate = new Date(
      startDate.getTime() + value * 24 * 60 * 60 * 1000
    );
    onDateChange(newDate);
  };

  return (
    <div className={styles.container}>
      <div className={styles.controls}>
        <button onClick={onPlayPause} className={styles.playButton}>
          {isPlaying ? "⏸" : "▶"}
        </button>
        <div className={styles.dateDisplay}>
          {currentDate.toLocaleDateString("en-US", {
            year: "numeric",
            month: "short",
            day: "numeric",
          })}
        </div>
      </div>
      <div className={styles.sliderContainer}>
        <input
          type="range"
          min="0"
          max={totalDays}
          value={currentDays}
          onChange={handleSliderChange}
          className={styles.slider}
        />
        <div className={styles.labels}>
          <span>
            {startDate.toLocaleDateString("en-US", {
              year: "numeric",
              month: "short",
            })}
          </span>
          <span>
            {endDate.toLocaleDateString("en-US", {
              year: "numeric",
              month: "short",
            })}
          </span>
        </div>
      </div>
    </div>
  );
}
