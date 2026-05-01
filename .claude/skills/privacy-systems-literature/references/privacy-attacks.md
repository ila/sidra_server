# Privacy Attacks and Disclosure Risks

## Membership Inference

- Pyrgelis, Troncoso, and De Cristofaro, "Knock Knock, Who's There? Membership Inference on Aggregate Location Data", arXiv:1708.06145. Shows membership inference against aggregate location time series. Source: https://arxiv.org/abs/1708.06145
- Shokri, Stronati, Song, and Shmatikov, "Membership Inference Attacks Against Machine Learning Models", IEEE S&P 2017 / arXiv:1610.05820. General MIA background. Source: https://arxiv.org/abs/1610.05820

SIDRA connection: PDS/staging MVs expose repeated aggregate releases, often by time/window and group. This is closer to aggregate time-series MIA than one-shot static tables.

## Reconstruction and Differencing

- Dinur and Nissim, "Revealing Information while Preserving Privacy", PODS 2003. Classic reconstruction result: too many too-accurate aggregate answers can reveal individual data.
- Dwork, McSherry, Nissim, and Smith, "Calibrating Noise to Sensitivity in Private Data Analysis", TCC 2006. Foundational DP mechanism paper.

SIDRA connection: Repeated flushes and overlapping windows create differencing attacks unless composition or release independence is handled carefully.

## OLAP and Information-Theoretic Disclosure

- Zhang and Zhao, "Privacy-preserving OLAP: An information-theoretic approach", 2007/2008 line. It treats OLAP aggregate disclosure and partial information leakage using information-theoretic controls. Source: https://pure.psu.edu/en/publications/privacy-preserving-olap-an-information-theoretic-approach/

SIDRA connection: Useful bridge between warehouse OLAP cells and PAC/PAC-like information-theoretic leakage.

## Practical SIDRA Controls

- Minimum aggregation reduces singling out but is not sufficient for repeated releases or outlier contribution attacks.
- Contribution bounding and clipping are required for sum/avg-style privacy mechanisms.
- Private key/group selection is required if the set of released groups itself is sensitive.
- PAC, DP, and k-thresholding answer different attack models. Keep those claims separate in the paper.
