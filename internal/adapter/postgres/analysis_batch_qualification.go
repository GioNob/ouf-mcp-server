//go:build batchload

package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// BatchQualificationStore attaches qualification instrumentation to the real store.
func BatchQualificationStore(ctx context.Context, cfg *pgxpool.Config) (*Store, error) {
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Store{pool: pool}, nil
}

// BatchQualificationPageSQL returns the statements used by the offer and SUMMARY readers.
func BatchQualificationPageSQL() (offer, summary string) {
	return batchReadyColumnsSQL, batchSummaryPageSQL
}
