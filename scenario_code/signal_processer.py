import heartpy as hp
import neurokit2 as nk
import numpy as np


class SignalProcessor:
    def process_bvp_signal(self, bvp_signal, fps):
        working_data, measures = hp.process(bvp_signal, fps)

        for x in measures:
            measures[x] = float(measures[x])

        return working_data, measures

    def get_psd_frequencies(
        self,
        peaks,
        rri,
        fps,
        ulf=(0, 0.0033),
        vlf=(0.0033, 0.04),
        lf=(0.04, 0.15),
        hf=(0.15, 0.4),
        vhf=(0.4, 0.5),
    ):
        hrv_indices = nk.hrv(peaks, sampling_rate=fps, show=False)

        # PSD plot
        rri, rri_time, psd_sampling_rate = nk.intervals_process(rri, interpolate=True)

        hrv_indices.columns = [col.replace("HRV_", "") for col in hrv_indices.columns]
        frequency_bands = hrv_indices[["ULF", "VLF", "LF", "HF", "VHF"]]
        frequency_band = [ulf, vlf, lf, hf, vhf]

        min_frequency = 0               # Check value
        psd = nk.signal_psd(
                rri,
                sampling_rate=psd_sampling_rate,
                show=False,
                min_frequency=min_frequency,
                method="welch",
                max_frequency=0.5,
                order_criteria=None,
                normalize=True,
                t=None,
            )
        
        # Sanitize signal
        if isinstance(frequency_band[0], int):
            if len(frequency_band) > 2:
                print(
                    "NeuroKit error: signal_power(): The `frequency_band` argument must be a list of tuples"
                    " or a tuple of 2 integers"
                )
            else:
                frequency_band = [tuple(i for i in frequency_band)]

        freq = np.array(psd["Frequency"])
        power = np.array(psd["Power"])

        # Get indexes for different frequency band
        frequency_band_index = []
        for band in frequency_band:
            indexes = np.logical_and(
                psd["Frequency"] >= band[0], psd["Frequency"] < band[1]
            )  # pylint: disable=E1111
            frequency_band_index.append(np.array(indexes))

        labels = list(frequency_bands.keys())
        # Reformat labels if of the pattern "Hz_X_Y"
        if len(labels[0].split("_")) == 3:
            labels = [i.split("_") for i in labels]
            labels = [f"{i[1]}-{i[2]} Hz" for i in labels]

        return freq, power, frequency_band_index, labels