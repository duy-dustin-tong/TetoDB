// config.h

// Why: Every other component needs to know "How big is a page?" or "What is the ID of an invalid page?"
// Define:
//     PAGE_SIZE (Standard is 4096 bytes or 4KB).
//     INVALID_PAGE_ID (Usually -1).
//     FRAME_ID_T (Type alias for int32_t).